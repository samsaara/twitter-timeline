# twitter-timeline
Simple Python Twitter crawler for user timelines

[![Build Status](https://travis-ci.org/vaddina/twitter-timeline.svg?branch=test)](https://travis-ci.org/vaddina/twitter-timeline)

Supports all options from the official [Twitter API](https://dev.twitter.com/rest/reference/get/statuses/user_timeline) documentation.

In addition:
* Uses just **Application-only** authentication - So no need of `Access Token & Secret` 😍. Rate limits are also higher. See [here](https://dev.twitter.com/oauth/application-only) for more info.
* you can also crawl only those tweets in the languages you prefer based on [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) codes.
* Remove the fields from the JSON response that you don't think are useful before storing them.

# Note:
This `test` branch is a bit more advanced and has more functionalities (listed below)... and is more suited for my needs. I don't plan to include all of these in `master` anytime soon but might incorporate some of the stuff from this into it. For now, feel free to tweak around. :)

Currently within this branch, you can fetch:
* [Top 1000](http://tvitre.no/norsktoppen) Norwegian twitteratis and their tweets
* Timelines of any public account maintaining twitteratis given by either their `screen_name` or `user_id`.
* Their (👆) followers (Default-Max: 100K) & their tweets and so on...
* Only new / recent tweets that you haven't crawled since last time.
* Supports **Multiple API Keys & Secrets**. The crawler switches the credentials whenever rate limits are reached for one of the API Key-Secret combinations...
so as to wait less until the limits are reset. With sufficient API Key-Secret pairs (say 5, to be safe), it is possible to crawl without waiting. (The rate limits are still respected in all cases)

You can also filter by your preferred language & JSON fields to store as mentioned above. 🙃

# Requirements:
* Needs *MongoDB* to store the tweets and a few other components. Check [here](requirements.txt) for more info.
* Store your `API Key(s) and API secret(s)` as pairs of raw strings on separate lines in a file (default name `.credentials`). E.g.,
```
'api_key_1'
'api_secret_1'
'api_key_2'
'api_secret_2'
...
```

 Do `pip install -r requirements.txt`


# Usage:
* ` $ python3 crawl_timelines.py --names 'elonmusk' `
* ` $ python3 crawl_timelines.py --names 'isro, sachin_rt' --lang 'en, hi'`
* ` $ python3 crawl_timelines.py --ids '2916305152, 20536157' --noFields 'geo, favorited, place' `

# License:

[MIT](LICENSE)
