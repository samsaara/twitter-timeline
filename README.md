# twitter-timeline
Simple Python Twitter crawler for user timelines

Supports all options from the official [Twitter API](https://dev.twitter.com/rest/reference/get/statuses/user_timeline) documentation.

In addition:
* Uses just **Application-only** authentication - So no need of `Access Token & Secret`. Rate limits are also higher. See [here](https://dev.twitter.com/oauth/application-only) for more info.
* you can also crawl only those tweets in the languages you prefer based on [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) codes.
* Remove the fields from the JSON response that you don't think are useful before storing them.

# Requirements:
* Needs *MongoDB* to store the tweets and a few other components. Check [here](requirements.txt) for more info.
* Store your `API Key and API secret` as raw strings on 2 separate lines in a file (default `.credentials`)

 Do `pip install -r requirements.txt`


# Note:
This 'test' branch is a bit more advanced and has more functionalities (like automatically fetching users, and their followers etc.)... and is more suited for my needs. I don't plan to include all of these in 'master' but might incorporate some of the stuff from this into it. For now, feel free to tweak around. :)


# Usage:
* ` $ python3 crawl_timelines.py --names 'elonmusk' `
* ` $ python3 crawl_timelines.py --names 'isro, sachin_rt' --lang 'en, hi'`
* ` $ python3 crawl_timelines.py --ids '2916305152, 20536157' --noFields 'geo, favorited, place' `

# License:

[MIT](LICENSE)
