# twitter-timeline
Simple Python Twitter crawler for user timelines

[![Build Status](https://travis-ci.org/vaddina/twitter-timeline.svg?branch=master)](https://travis-ci.org/vaddina/twitter-timeline)

Supports all options from the official [Twitter API](https://dev.twitter.com/rest/reference/get/statuses/user_timeline) documentation.

In addition:
* Uses just **Application-only** authentication - So no need of `Access Token & Secret` 😍. Rate limits are also higher. See [here](https://dev.twitter.com/oauth/application-only) for more info.
* you can also crawl only those tweets in the languages you prefer based on [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) codes.
* Remove the fields from the JSON response that you don't think are useful before storing them.

# Note:
There's a `test` branch with more advanced options and features. Read [this](https://github.com/vaddina/twitter-timeline/blob/test/README.md#note) if you want to know more.

# Requirements:
* Needs *MongoDB* to store the tweets and a few other components. Check [here](requirements.txt) for more info.
* Store your `API Key and API secret` as raw strings on 2 separate lines in a file (default `.credentials`)

 Do `pip install -r requirements.txt`

# Usage:
* ` $ python3 crawl_timelines.py --names 'elonmusk' `
* ` $ python3 crawl_timelines.py --names 'isro, sachin_rt' --lang 'en, hi'`
* ` $ python3 crawl_timelines.py --ids '2916305152, 20536157' --noFields 'geo, favorited, place' `

# License:

[MIT](LICENSE)
