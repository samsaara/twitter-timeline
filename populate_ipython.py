from pymongo import MongoClient
client = MongoClient()
tdb = client.twitter
tcol = tdb.timeline
toc = tdb.to_crawl
crld = tdb.crawled
