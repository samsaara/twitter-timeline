# -*- coding: utf-8 -*-
""" core utils """

import base64
import time
import pandas as pd
import urllib
import urllib.request
import ujson
from bson.int64 import Int64

import logging
logging.basicConfig(filename="crawler.log", level=logging.DEBUG,
                    format='%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class Util:

    def __init__(self, credentials_file = '.credentials'):
        self.API_VERSION = '1.1'
        self.BASE_URL = 'https://api.twitter.com/{}'.format(self.API_VERSION)
        self.TOKEN_URL = 'https://api.twitter.com/oauth2/token'
        self.TIMELINE_URL = '{}/statuses/user_timeline.json'.format(self.BASE_URL)

        self.ACCESS_TOKENS = self.get_access_tokens(credentials_file)


    def get_credentials(self, credentials_file):
        """ Loads API's Key & Secret """

        log.debug('loading app credentials...')
        with open(credentials_file) as fl:
            contents = fl.read()

        api_keys = contents.splitlines()[::2]
        api_secrets = contents.splitlines()[1::2]

        assert len(api_keys) == len(api_secrets), 'unequal api keys & secrets... number should be even'
        return api_keys, api_secrets


    def get_access_tokens(self, credentials_file):
        """ Gets bearer (access) token for crawling the timelines """

        api_keys, api_secrets = self.get_credentials(credentials_file)
        tokens = []
        for i in range(len(api_keys)):
            b64enc = base64.b64encode('{}:{}'.format(api_keys[i], api_secrets[i]).encode('ascii'))
            auth = 'Basic {}'.format(b64enc.decode('utf-8'))
            data = urllib.parse.urlencode({'grant_type':'client_credentials'}).encode('utf-8')
            headers = {'Authorization': auth,
                        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
            req = urllib.request.Request(self.TOKEN_URL, headers=headers, data=data)

            try:
                log.debug('fetching bearer token...')
                with urllib.request.urlopen(req) as op:
                    resp = op.read()
                tokens.append(ujson.loads(resp.decode('utf8'))['access_token'])

            except:
                log.exception("Error fetching the bearer token. Re-check app: [{}] credentials !!!".format(i))
                return None

        return tokens


    def check_rate_limit_status(self, criteria='timeline', app=0):
        """ returns the remaining number of calls and the reset time of the counter for
            'timeline / user_lookup / follower ids'
        """

        dc = {'timeline':'statuses', 'user_lookup':'users', 'followers':'followers', 'friends':'friends'}
        url = '{}/application/rate_limit_status.json?resources={}'.format(self.BASE_URL, dc.get(criteria))

        auth = 'Bearer {}'.format(self.ACCESS_TOKENS[app])
        header = {'Authorization': auth}
        req = urllib.request.Request(url, headers=header)

        resp = None
        try:
            log.info('fetching rate_limit_status...')
            with urllib.request.urlopen(req) as op:
                resp = op.read()
        except:
            log.exception('Error in getting the rate limits !!!')
            return None

        resp = ujson.loads(resp.decode('utf8'))

        if criteria == 'timeline':
            limits = resp['resources']['statuses']['/statuses/user_timeline']
        elif criteria == 'user_lookup':
            limits = resp['resources']['users']['/users/lookup']
        elif criteria == 'followers':
            limits = resp['resources']['followers']['/followers/ids']
        elif criteria == 'friends':
            limits = resp['resources']['friends']['/friends/ids']

        rem_hits, reset_time = limits.get('remaining'), limits.get('reset')

        return rem_hits, reset_time


    def strip_entities(self, ent={}):
        """ Strip all the stuff except "indices" from the 'entities' in a JSON response from twitter """

        dc = {}
        for key in ent.keys():
            vals = ent.get(key)
            if vals:
                indices = []
                for i in vals:
                    indices.append(i['indices'])
                dc[key] = indices
            else:
                dc[key] = []

        return dc


    def get_user_ids(self, usernames, app=0):
        """ Gets user_ids given list of screen_names """

        url = '{}/users/lookup.json?screen_name={}'.format(self.BASE_URL, ','.join(usernames))
        auth = 'Bearer {}'.format(self.ACCESS_TOKENS[app])
        header = {'Authorization': auth}
        req = urllib.request.Request(url, headers=header)

        resp = None
        try:
            log.debug('fetching userids for the screen_names...')
            with urllib.request.urlopen(req) as op:
                resp = op.read()
        except:
            log.exception('Error in getting the rate limits !!!')
            return None

        df =  pd.read_json(resp.decode('utf8'))[['id', 'screen_name']]
        df.rename(columns={'id':'_id'}, inplace=True)

        return df.to_dict(orient='records')


    def get_people(self, rem_hits, reset_time, user_id=None, screen_name=None, levels=-1, app=0, people='followers'):
        """ Get the followers' / friends userids (in chunks of 5000 - each level: one chunk, max: 100K/ 20 chunks) for any given user. Specify 'levels=-1' for that...

            People: 'followers' / 'friends'
        """
        quota_full = 0
        tot_tokens = len(self.ACCESS_TOKENS)

        if levels == -1:
            levels = 20     # Download up to 100K followers max for any user.

        level = 0
        cursor = -1
        IDs = []

        log.debug('fetching {} for user {}...'.format(people, user_id if user_id else screen_name))
        while True:
            if not (level < levels and cursor != 0):
                break

            if rem_hits > 0:
                url = '{}/{}/ids.json?cursor={}&{}={}&count=5000'.format(self.BASE_URL, people, cursor,
                                        'user_id' if user_id else 'screen_name', user_id if user_id else screen_name)
                auth = 'Bearer {}'.format(self.ACCESS_TOKENS[app])
                header = {'Authorization': auth}
                req = urllib.request.Request(url, headers=header)
                resp = None
                try:
                    with urllib.request.urlopen(req) as op:
                        resp = op.read()
                except:
                    log.exception('Error in fetching the followers !!!')
                    break

                rem_hits -= 1

                if resp:
                    dc = ujson.loads(resp.decode('utf8'))
                    IDs += dc["ids"]
                    cursor = dc['next_cursor']
                    log.debug('next_cursor: {}'.format(cursor))
                else:
                    break

                level += 1
                time.sleep(.01)
                quota_full = 0

            else:
                app += 1
                quota_full += 1
                if app % tot_tokens == 0:
                    app = 0

                if quota_full == tot_tokens:
                    sleep = 15 * 60
                    wakeup_time = pd.datetime.ctime(pd.datetime.now() + pd.Timedelta(sleep, 's'))
                    log.info('user_lookup: sleeping for {} minutes... waking up at: {}'.format(round(sleep/60, 2),
                                                                                                wakeup_time))
                    # Sleep for one more second to wait for the reset of the limits
                    time.sleep(sleep+1)
                    quota_full = 0

                rem_hits, reset_time = self.check_rate_limit_status(criteria=people, app=app)
                log.debug('\n\n Util.get_people: swtiched to app: {}. New rem_hits: {}, reset_time: {} \
                            \n\n'.format(app, rem_hits, time.ctime(reset_time)))

        log.debug('got {} {} for user: {}'.format(len(IDs), people, user_id if user_id else screen_name))
        return pd.DataFrame(IDs, columns=['_id'], dtype=Int64).to_dict(orient='records'), rem_hits, reset_time, app


    def get_top_twitteratis(self, url="http://tvitre.no/norsktoppen"):
        """ Gets the top 1000 users from http://tvitre.no/norsktoppen """

        from bs4 import BeautifulSoup as bs

        user_agent = "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7"
        hdr = {'User-Agent': user_agent}
        twitteratis = []

        def _collect_usernames(url):
            log.debug("Searching for usernames on page: {}".format(url))
            try:
                website = urllib.request.urlopen(urllib.request.Request(url, headers=hdr))
            except:
                log.exception("error crawling for usernames on page: {}".format(url))
                return -1

            content = website.read()
            soup = bs(content, 'html.parser')

            # Get twitteratis' usernames (they start with "@")
            twitteratis.extend(list(filter(bool, [link.getText().lstrip('@') if link.getText().startswith('@')
                                            else '' for link in soup.find_all('a')])))

            # Get the link to the next page
            next_tag = soup.find_all("li", {"class": "next"})
            if next_tag:
                next_url = next_tag[0].find_all('a')[0].get('href')
                url = urllib.parse.urljoin(url, next_url)
                return _collect_usernames(url)
            else:
                return twitteratis

        return _collect_usernames(url)



if __name__ == '__main__':
    ut = Util()
    for i in range(len(ut.ACCESS_TOKENS)):
        print (ut.check_rate_limit_status(app=i))

    # twitteratis = ut.get_top_twitteratis()
    # print (twitteratis[:10])
