# -*- coding: utf-8 -*-

__author__ = 'Vaddina'

import argparse
import time
import ujson
from bson.int64 import Int64
import urllib
import urllib.request
from urllib.error import HTTPError

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from utils import Util

import pandas as pd
pd.set_option('display.expand_frame_repr', False)

import logging
logging.basicConfig(filename="crawler.log", level=logging.DEBUG,
                    format='%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class Crawler:

    def __init__(self, screen_names=[], user_ids=[], count=200, exclude_replies=True, exclude_fields=None,
                    contributor_details=False, include_rts=False, db='twitter', port=27017, host='localhost',
                    collection='timeline', pref_langs=['en', 'no', 'nn', 'nb']):

        self.screen_names = screen_names
        self.user_ids = user_ids
        self.count = count
        self.exclude_replies = exclude_replies
        self.contributor_details = contributor_details
        self.include_rts = include_rts
        self.pref_langs = pref_langs
        self.exclude_fields = exclude_fields

        # Get all the utility functions
        self.util = Util()

        try:
            self.client = MongoClient(host, port)
        except:
            log.exception('Failed connecting to the database !!!')
            return

        self.db = self.client[db]
        self.collection = self.db[collection]       # Main collection for storing tweets & metadata
        self.buffer = self.db['buffer']             # Temporary collection to store screen_names
        self.to_crawl = self.db['to_crawl']         # Collection to store user_ids to crawl
        self.crawled = self.db['crawled']           # Collection to store user_ids that have been crawled

        # Boolean Variable that's set to True once a few indices (listed below in 'store_in_db') are created / detected
        self.created_indices = False


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
        params += '&exclude_replies={}'.format(str(self.exclude_replies).lower())
        params += '&contributor_details={}'.format(str(self.contributor_details).lower())
        # TODO: Decide whether this to be included or not...
        params += '&include_rts={}'.format(str(self.include_rts).lower())

        full_timeline_url = self.util.TIMELINE_URL + params
        # log.debug("\nquery: \n{}\n".format(full_timeline_url))
        auth = 'Bearer {}'.format(self.util.ACCESS_TOKEN)
        header = {'Authorization': auth}
        req = urllib.request.Request(full_timeline_url, headers=header)

        log.info('fetching timeline of user: {}'.format(screen_name if screen_name else user_id))
        try:
            with urllib.request.urlopen(req) as op:
                resp = op.read()

            # Return raw response...
            return resp.decode('utf8')

        except HTTPError:
            log.error ('Not possible to crawl... may be a protected user')

        return None


    def get_since_id(self, user_id):
        """ Finds the most recent tweet ID of any given user in DB """

        return self.collection.find_one({'user':user_id}, sort=[('_id', -1)])


    def _clean_response(self):
        """ removes unnecessary fields and formats the data - fit to feed in DB """

        self.max_id = self.df.id.min() - 1
        if self.pref_langs:
            # Extract only those tweets that are in one of preferred languages...
            self.df = self.df[self.df.lang.isin(self.pref_langs)]

        if len(self.df):
            # *** WARNING: Uncomment this when appropriate... Currently commented to increase speed. ***

            # if self.exclude_fields:
            #     if 'id' in self.exclude_fields:
            #         log.warning("can't remove ID field. Required for efficient crawling !!!")
            #         self.exclude_fields.remove('id')
            rem_fields = list(set(self.df.columns) - set(self.exclude_fields))
            self.df = self.df.get(rem_fields)

            # if 'entities' in self.df.columns:
                # strip entities
            self.df.entities = self.df.entities.apply(lambda x: self.util.strip_entities(x))
            self.df.user = self.df.user.apply(lambda x: {'id': x.get('id'), 'screen_name': x.get('screen_name')})
            # if 'id_str' in self.df.columns:
            #     self.df.id_str = self.df.id_str.astype(str)

            self.df.rename(columns={'id':'_id'}, inplace=True)
            log.debug('Got {} tweets'.format(len(self.df)))

            self.df = self.df.to_dict(orient='records')

            log.debug('cleaned response...')
            return True

        else:
            log.warning('no tweets found in preferred language...')
            return False


    def empty_buffer(self):
        """ Gets the user_ids of the screen_names in 'buffer' collection and stores them in 'to_crawl' collection """

        rem_hits, reset_time = self.util.check_rate_limit_status(criteria='user_lookup')
        while time.time() < reset_time:
            if self.buffer.count() == 0:
                break

            if rem_hits > 0:
                chunk = pd.DataFrame(list(self.buffer.find().limit(100)))
                self.user_ids = self.util.get_user_ids(chunk._id.tolist())
                self.store_in_db(collection='to_crawl', with_screen_name=True)
                rem_hits -= 1

                self.buffer.delete_many({'_id': {'$in': chunk._id.tolist()}})
                time.sleep(.01)

            else:
                sleep = reset_time - time.time()
                wakeup_time = pd.datetime.ctime(pd.datetime.now() + pd.Timedelta(sleep, 's'))
                log.info('user_lookup: sleeping for {} minutes... waking up at: {}'.format(round(sleep/60, 2),
                                                                                            wakeup_time))
                # Sleep for one more second to wait for the reset of the limits
                time.sleep(sleep+1)
                rem_hits, reset_time = self.util.check_rate_limit_status(criteria='user_lookup')

        log.debug('"buffer" emptied...')


    def _get_name_or_id(self):
        dc = self.to_crawl.find_one()

        while True:
            if dc is not None:
                screen_name, user_id = dc.get('screen_name'), dc.get('_id')
                if self.crawled.find_one({'_id': user_id}):
                    log.debug('User "{}" already crawled...so skipping now...'.format(screen_name if screen_name else
                                                                                    user_id))
                    self.to_crawl.delete_many({'_id': user_id})
                    dc = self.to_crawl.find_one()
                    continue
                else:
                    return screen_name, user_id
            else:
                log.info('\n\n all user_ids crawled... get me more ids !!! \n\n')
                return None, None


    def fill_with_followers(self, user_id=None, screen_name=None, from_crawled=False, levels=1):
        """ Get the followers' userids (in chunks of 5000 - each level: one chunk, max: 100K/ 20 chunks) for any given
            user. Specify 'levels=-1' for that...

            If 'from_crawled' is set, then it gets the follower_ids for each of the crawled users
        """

        rem_hits, reset_time = self.util.check_rate_limit_status(criteria='followers')

        if not from_crawled:
            df, _, _ = self.util.get_followers(rem_hits, reset_time, user_id, screen_name, levels)
            try:
                self.to_crawl.insert_many(df, ordered=False)
            except BulkWriteError:
                log.warning('some user_ids seem to already exist...')

            log.debug('added followers of user {} to DB'.format(screen_name if screen_name else user_id))

        else:
            cur = self.crawled.find()
            while True:
                log.debug('top: rem_hits: {}, reset_time: {}, now: {}'.format(rem_hits, reset_time, time.time()))
                if (rem_hits > 0) and (time.time() < reset_time):
                    try:
                        _id = next(cur).get('_id')
                    except StopIteration:
                        break

                    df, rem_hits, reset_time = self.util.get_followers(rem_hits, reset_time, user_id=_id, levels=levels)

                    try:
                        self.to_crawl.insert_many(df, ordered=False)
                    except BulkWriteError:
                        log.warning('some user_ids seem to already exist...')
                else:
                    sleep = reset_time - time.time()
                    wakeup_time = pd.datetime.ctime(pd.datetime.now() + pd.Timedelta(sleep, 's'))
                    log.info('Followers_crawl: sleeping for {} minutes... waking up at: {}'.format(round(sleep/60, 2),
                                                                                                    wakeup_time))
                    # Sleep for one more second to wait for the reset of the limits
                    time.sleep(sleep+1)
                    rem_hits, reset_time = self.util.check_rate_limit_status(criteria='followers')

            log.debug("fetched followers for ids in 'crawled' upto level: {}".format(levels))

    def crawl(self, top_users=False, only_new=False):
        """ Efficient crawl for twitter timelines. """

        # small hack to make the function call compatible with both 'screen_names' / 'user_ids'...
        if self.screen_names:
            self.store_in_db(collection='buffer')
        elif self.user_ids:
            self.store_in_db(collection='to_crawl')
        elif top_users:
            self.screen_names = self.util.get_top_twitteratis()
            self.store_in_db(collection='buffer')

        # TODO: Implement 'only_new' functionality

        # Empty the buffer collection first...
        self.empty_buffer()

        while True:
            screen_name, user_id = self._get_name_or_id()
            if not (screen_name or user_id):
                break

            log.info('crawling for "{}"'.format(screen_name if screen_name else user_id))
            self.max_id, self.since_id = None, None
            rem_hits, reset_time = self.util.check_rate_limit_status()

            while time.time() < reset_time:
                if rem_hits > 0:
                    resp = self._get_timeline(screen_name, user_id)
                    rem_hits -= 1

                    if resp != '[]' and resp is not None:
                        self.df = pd.read_json(resp)
                        store = self._clean_response()
                        self.store_in_db() if store else None
                    else:
                        log.info('crawling finished for user {}'.format(screen_name if screen_name else user_id))
                        self.crawled.insert_one({'_id': user_id})
                        self.to_crawl.delete_one({'_id': user_id})
                        break

                    time.sleep(.01)

                else:
                    interm_hits, interm_time = self.util.check_rate_limit_status()
                    if interm_time > reset_time:
                        rem_hits, reset_time = interm_hits, interm_time
                    else:
                        sleep = interm_time - time.time()
                        wakeup_time = pd.datetime.ctime(pd.datetime.now() + pd.Timedelta(sleep, 's'))
                        log.info('sleeping for {} minutes... waking up at: {}'.format(round(sleep/60, 2), wakeup_time))
                        # Sleep for one more second to wait for the reset of the limits
                        time.sleep(sleep+1)
                        rem_hits, reset_time = self.util.check_rate_limit_status()

        log.info('exiting...\n\n')


    def store_in_db(self, collection='tweets', with_screen_name=False):
        """ Stores fetched & preprocessed tweets in DB """

        try:
            if collection == 'tweets':
                self.collection.insert_many(self.df, ordered=False)

                if not self.created_indices:
                    if 'created_at_1' not in self.collection.index_information().keys():
                        log.debug('\n\n creating indices \n\n')
                        self.collection.create_index('created_at')

                    if 'user.id_1' not in self.collection.index_information().keys():
                        self.collection.create_index('user.id')

                    if 'user.screen_name_1' not in self.collection.index_information().keys():
                        self.collection.create_index('user.screen_name')

                    self.created_indices = True

            elif collection == 'buffer':
                fd = pd.DataFrame(self.screen_names, columns=['_id'], dtype=Int64).to_dict(orient='records')
                self.buffer.insert_many(fd, ordered=False)

                log.debug('inserted in "buffer"')

            elif collection == 'to_crawl':
                if not with_screen_name:
                    fd = pd.DataFrame(self.user_ids, columns=['_id'], dtype=Int64).to_dict(orient='records')
                else:
                    fd = self.user_ids

                self.to_crawl.insert_many(fd, ordered=False)

                log.debug('inserted in "to_crawl"')

        except BulkWriteError:
            log.warning('some rows seem to already exist.. not updating them...')

        log.debug('successfully stored in DB !!!')
        return


    def drop_collection(self, collection):
        log.warning('request received to drop Collection "{}"...'.format(collection))
        available_cols = self.db.collection_names()
        if not collection in available_cols:
            log.error('"{}" collection not found... avalable: {}'.format(collection, available_cols))
            return

        inp = None
        while inp not in ['yes', 'y', 'no', 'n']:
            inp = input('\n Drop the collection "{}"? (yes/no): '.format(collection)).lower()
        if inp in ['y', 'yes']:
            self.db.drop_collection(collection)
            print('Collection dropped !!!')
            log.warning('Collection dropped !!!')
        else:
            log.warning('Collection NOT dropped !!!')


    def drop_database(self, database):
        log.warning('request received to drop Database {}...'.format(database))
        available_dbs = self.client.database_names()
        if database not in available_dbs:
            log.error('"{}" database not found... avalable: {}'.format(database, available_dbs))
            return

        inp = None
        while inp not in ['yes', 'y', 'no', 'n']:
            inp = input('\n Drop the Database "{}"? (yes/no): '.format(database)).lower()
        if inp in ['y', 'yes']:
            self.client.drop_database(database)
            print('Database dropped !!!')
            log.warning('Database dropped !!!')
        else:
            log.warning('Database NOT dropped !!!')



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--names", help='screen_names separated by "," (this or "--ids" option is mandatory). Takes precedence over "--ids"')
    parser.add_argument("-i", "--ids", help='user_ids separated by "," (this or "--names" option is mandatory)')
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
    # if not (args.names or args.ids):
    #     parser.error('Need either "screen_names" or "user_ids" to crawl the timeline of.')

    if args.names:
        args.names = [name.strip() for name in args.names.strip().split(',') if len(name)]
        args.ids = []
    elif args.ids:
        args.ids = [abs(int(_id)) for _id in args.ids.split(',')]
        args.names = []

    if args.lang:
        args.lang = [lang.strip() for lang in args.lang.strip().split(',') if len(lang)]
    else:
        args.lang = ['en', 'no', 'nn', 'nb']

    if args.noFields:
        args.noFields = [field.strip() for field in args.noFields.strip().split(',') if len(field)]
    else:
        args.noFields = ['contributors', 'coordinates', 'extended_entities', 'favorite_count', 'favorited',
                            'geo', 'id_str', 'in_reply_to_screen_name', 'place', 'in_reply_to_status_id',
                            'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',
                            'is_quote_status', 'possibly_sensitive', 'retweet_count', 'retweeted', 'source',
                            'quoted_status', 'quoted_status_id', 'quoted_status_id_str',
                        ]

    try:
        crawler = Crawler(screen_names=args.names, user_ids=args.ids, exclude_fields=args.noFields,
                        exclude_replies=args.noExReps, contributor_details=args.contrib, include_rts=args.retweets,
                        db=args.db, host=args.host, port=args.port, collection=args.collection, pref_langs=args.lang)

        # crawler.crawl(top_users=True)
        crawler.fill_with_followers(user_id=141968149, levels=2)

    except:
        log.exception('Error !!! Closing down DB connections, if any..')
    finally:
        try:
            if hasattr(crawler, 'client'):
                crawler.client.close()
        except:
            pass
