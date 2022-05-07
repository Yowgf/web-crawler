# CrawlShard is the class that defines a crawling job. Each thread receives a
# CrawlShard object to figure out which pages it needs to crawl.
#
# Each shard only refers to a specific host (maybe in the future we can accept
# more than one?), to avoid thread conflict and IP-blacklisting.
class CrawlShard:
    def __init__(self, host, delay, tocrawl):
        self.host = host
        self.delay = delay
        self.tocrawl = tocrawl
