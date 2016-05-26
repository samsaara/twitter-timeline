# twitter-timeline
Simple Python Twitter crawler for user timelines

Supports all options from the official [Twitter API](https://dev.twitter.com/rest/reference/get/statuses/user_timeline) documentation.

In addition:
* you can also crawl only those tweets in the languages you prefer based on [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) codes.
* Remove the fields from the JSON response that you don't think are useful before storing them.

# Requirements:
 Needs *MongoDB* to store the tweets and a few other components. Check [here](requirements.txt) for more info. 
 
 Do `pip install -r requirements.txt`
 
# Usage:
* ` $ python crawl_timelines.py --names 'elonmusk' `
* ` $ python crawl_timelines.py --names 'isro, sachin_rt' --lang 'en, hi'`
* ` $ python crawl_timelines.py --ids '2916305152, 20536157' --noFields 'geo, favorited, place' `

# License:

[MIT](LICENSE)
