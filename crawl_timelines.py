# -*- coding: utf-8 -*-

__author__ = 'Vaddina'

import argparse
import base64
import time
import ujson
import urllib
import urllib.request

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

import pandas as pd
pd.set_option('display.expand_frame_repr', False)

import logging
logging.basicConfig(filename="crawler.log", level=logging.DEBUG,
                    format='%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)

API_VERSION = '1.1'
TOKEN_URL = 'https://api.twitter.com/oauth2/token'
BASE_URL = 'https://api.twitter.com/{}'.format(API_VERSION)
TIMELINE_URL = '{}/statuses/user_timeline.json'.format(BASE_URL)


def get_credentials(filename='.credentials'):
    """ Loads API's Key & Secret """

    log.debug('loading app credentials...')
    with open(filename) as fl:
        contents = fl.read()

    api_key, api_secret, *k = contents.split('\n')

    return api_key, api_secret


def get_access_token():
    """ Gets bearer (access) token for crawling the timelines """

    api_key, api_secret = get_credentials()
    b64enc = base64.b64encode('{}:{}'.format(api_key, api_secret).encode('ascii'))
    auth = 'Basic {}'.format(b64enc.decode('utf-8'))
    data = urllib.parse.urlencode({'grant_type':'client_credentials'}).encode('utf-8')
    headers = {'Authorization': auth,
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
    req = urllib.request.Request(TOKEN_URL, headers=headers, data=data)

    try:
        log.debug('fetching bearer token...')
        with urllib.request.urlopen(req) as op:
            resp = op.read()
        access_token = ujson.loads(resp.decode('utf8'))['access_token']
        return access_token

    except:
        log.exception("Error fetching the bearer token. Re-check app's credentials !!!")
        return None


class Crawler:

    def __init__(self, screen_names=[], user_ids=[], count=200, trim_user=True, exclude_replies=True,
                    contributor_details=False, include_rts=False, db='twitter', port=27017, host='localhost', collection='timeline', pref_langs=['en', 'no', 'nn', 'nb']):

        self.screen_names = screen_names
        self.user_ids = user_ids
        self.count = count
        self.trim_user = trim_user
        self.exclude_replies = exclude_replies
        self.contributor_details = contributor_details
        self.include_rts = include_rts
        self.pref_langs = pref_langs

        self.ACCESS_TOKEN = get_access_token()

        try:
            self.client = MongoClient(host, port)
        except:
            log.exception('Failed connecting to the database !!!')
            return

        self.db = self.client[db]
        self.collection = self.db[collection]


    def check_rate_limit_status(self):
        """ returns the remaining number of calls and the reset time of the counter """

        url = '{}/application/rate_limit_status.json?resources=statuses'.format(BASE_URL)
        auth = 'Bearer {}'.format(self.ACCESS_TOKEN)
        header = {'Authorization': auth}
        req = urllib.request.Request(url, headers=header)

        resp = None
        try:
            log.debug('fetching rate_limit_status...')
            with urllib.request.urlopen(req) as op:
                resp = op.read()
        except:
            log.exception('Error in getting the rate limits !!!')
            return None

        resp = ujson.loads(resp.decode('utf8'))

        timeline_limits = resp['resources']['statuses']['/statuses/user_timeline']
        rem_hits, reset_time = timeline_limits.get('remaining'), timeline_limits.get('reset')

        return rem_hits, reset_time


    def _get_timeline(self, screen_name=None, user_id=None):
        """ Main workhorse for crawling the timelines """

        if not (screen_name or user_id):
            log.exception("'screen_name' or 'user_id' required !!! ")
            return None

        if screen_name:
            params = '?screen_name={}'.format(screen_name)
        else:
            params = '?user_id={}'.format(user_id)

        if self.since_id:
            params += '&since_id={}'.format(self.since_id)

        if self.max_id:
            params += '&max_id={}'.format(self.max_id)

        params += '&count={}'.format(self.count)
        params += '&trim_user={}'.format(str(self.trim_user).lower())
        params += '&exclude_replies={}'.format(str(self.exclude_replies).lower())
        params += '&contributor_details={}'.format(str(self.contributor_details).lower())
        params += '&include_rts={}'.format(str(self.include_rts).lower())

        full_timeline_url = TIMELINE_URL + params
        # log.debug("\nquery: \n{}\n".format(full_timeline_url))
        auth = 'Bearer {}'.format(self.ACCESS_TOKEN)
        header = {'Authorization': auth}
        req = urllib.request.Request(full_timeline_url, headers=header)

        try:
            log.debug('fetching timeline of user: {}'.format(screen_name if screen_name else user_id))
            with urllib.request.urlopen(req) as op:
                resp = op.read()

            # Return raw response...
            return resp.decode('utf8')

        except:
            log.exception("\n Error crawling the timeline !!! \n")
            return None


    def store_in_db(self):
        """ Stores fetched & preprocessed tweets in DB """

        if 'created_at' in self.dfColumns:
            self.collection.create_index('created_at')

        try:
            self.collection.insert_many(self.dfJson, ordered=False)
        except BulkWriteError:
            log.warning('some rows seem to already exist.. not updating them...')

        log.debug('successfully stored in DB !!!')
        return

    def _create_generator(self):
        for screen_name, user_id in zip(self.screen_names, self.user_ids):
            yield screen_name.lstrip('@'), user_id


    def get_since_id(self, screen_name, user_id):
        """ Finds the most recent tweet ID of any given user in DB """

        if screen_name:
            res = self.collection.find_one({'screen_name':screen_name}, sort=[('_id', -1)])
        else:
            res = self.collection.find_one({'user.id':user_id}, sort=[('_id', -1)])

        if res is not None:
            return res['_id']
        else:
            return None


    def crawl(self, exclude_fields=None):
        """ Efficient crawl for twitter timelines.
            Pass fields in 'exclude_fields' to remove them from returned JSON response from Twitter
        """

        # small hack to make the function call compatible with either of the params...
        if self.screen_names:
            self.user_ids = [None] * len(self.screen_names)
        else:
            self.screen_names = [None] * len(self.user_ids)

        generate_user = self._create_generator()
        screen_name, user_id = next(generate_user)

        self.max_id, self.since_id = None, self.get_since_id(screen_name, user_id)
        rem_hits, reset_time = self.check_rate_limit_status()
        while time.time() < reset_time:
            if rem_hits > 0:
                resp = self._get_timeline(screen_name, user_id)
                rem_hits -= 1

                if resp != '[]' and resp is not None:
                    df = pd.read_json(resp)
                    # log.debug('len(df): {}'.format(len(df)))
                    self.max_id = df.id.min() - 1

                    if self.pref_langs:
                        # Extract only those tweets that are in one of preferred languages...
                        df = df[df.lang.isin(self.pref_langs)]

                    if len(df):
                        if screen_name:
                            df['screen_name'] = screen_name

                        if exclude_fields:
                            if 'id' in exclude_fields:
                                log.warning("can't remove ID field. Required for efficient crawling !!!")
                                exclude_fields.remove('id')
                            rem_fields = list(set(df.columns) - set(exclude_fields))
                            df = df.get(rem_fields)
                        if 'id_str' in df.columns:
                            df.id_str = df.id_str.astype(str)

                        df.rename(columns={'id':'_id'}, inplace=True)
                        self.dfColumns = df.columns

                        log.debug('Got {} tweets, max_id: {}'.format(len(df), self.max_id))

                        self.dfJson = df.to_dict(orient='records')
                        self.store_in_db()
                    else:
                        log.warning('no tweets found in preferred language...')

                else:
                    try:
                        screen_name, user_id = next(generate_user)
                    except StopIteration:
                        log.info('crawling finished...')
                        break

                    log.debug('getting next user: {}'.format(screen_name if screen_name else user_id))
                    self.max_id, self.since_id = None, self.get_since_id(screen_name, user_id)

                time.sleep(.01)

            else:
                sleep = reset_time - time.time()
                wakeup_time = pd.datetime.ctime(pd.datetime.now() + pd.Timedelta(sleep, 's'))
                log.debug('sleeping for {} minutes... waking up at: {}'.format(round(sleep/60, 2), wakeup_time))
                # Sleep for one more second to wait for the reset of the limits
                time.sleep(sleep+1)
                rem_hits, reset_time = self.check_rate_limit_status()

        log.debug('exiting...')


    def drop_collection(self):
        log.warning('request received to drop Collection "{}"...'.format(self.collection.name))
        inp = None
        while inp not in ['yes', 'y', 'no', 'n']:
            inp = input('\n Drop the collection "{}"? (yes/no): '.format(self.collection.name)).lower()
        if inp in ['y', 'yes']:
            self.db.drop_collection(self.collection)
            print('Collection dropped !!!')
            log.warning('Collection dropped !!!')
        else:
            log.warning('Collection NOT dropped !!!')


    def drop_database(self):
        log.warning('request received to drop Database {}...'.format(self.db.name))
        inp = None
        while inp not in ['yes', 'y', 'no', 'n']:
            inp = input('\n Drop the Database "{}"? (yes/no): '.format(self.db.name)).lower()
        if inp in ['y', 'yes']:
            self.client.drop_database(self.db)
            print('Database dropped !!!')
            log.warning('Database dropped !!!')
        else:
            log.warning('Database NOT dropped !!!')



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--names", help='screen_names separated by "," (this or "--ids" option is mandatory). Takes precedence over "--ids"')
    parser.add_argument("-i", "--ids", help='user_ids separated by "," (this or "--names" option is mandatory)')
    parser.add_argument("-r", "--noTrim", action='store_false', help="Don't trim user details !!! (default: False)",
                        default=True)
    parser.add_argument("-x", "--noExReps", action='store_false', help='Do Not exclude replies while crawling tweets \
                        (Default: True)', default=True)
    parser.add_argument("-c", "--contrib", action='store_true', help='Include Contributor details (Default: False)',
                        default=False)
    parser.add_argument("-l", "--retweets", action='store_true', help='Include retweets (Default: False)',
                        default=False)

    parser.add_argument("-o", "--host", help='host for the MongoClient to connect', default='localhost')
    parser.add_argument("-p", "--port", help='port for the MongoClient to connect', type=int, default=27017)
    parser.add_argument("-d", "--db", help='Name of the DB to connect to (Default: "twitter")', default='twitter')
    parser.add_argument("-b", "--collection", help='Name of the Collection of DB to connect to (Default: "timeline")',
                        default='timeline')
    parser.add_argument("-g", "--lang", help='list of preferred languages (separated by ",") to collect the tweets \
                        in. Should be in "ISO 639-1" format. See https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes \
                        E.g. "en, fr, de" (Default: None)', default=None)

    parser.add_argument("-f", "--noFields", help='fields to ignore (separated by ",")')


    args = parser.parse_args()
    if not (args.names or args.ids):
        parser.error('Need either "screen_names" or "user_ids" to crawl the timeline of.')

    if args.names:
        args.names = [name.strip() for name in args.names.strip().split(',') if len(name)]
        args.ids = []
    else:
        args.ids = [abs(int(_id)) for _id in args.ids.split(',')]
        args.names = []

    if args.lang:
        args.lang = [lang.strip() for lang in args.lang.strip().split(',') if len(lang)]

    if args.noFields:
        args.noFields = [field.strip() for field in args.noFields.strip().split(',') if len(field)]

    try:
        crawler = Crawler(screen_names=args.names, user_ids=args.ids, trim_user=args.noTrim,
                            exclude_replies=args.noExReps, contributor_details=args.contrib, include_rts=args.retweets, db=args.db, host=args.host, port=args.port, collection=args.collection, pref_langs=args.lang)

        crawler.crawl(exclude_fields=args.noFields)

    except:
        log.exception('Error !!! Closing down DB connections, if any..')
    finally:
        if hasattr(crawler, 'client'):
            crawler.client.close()
