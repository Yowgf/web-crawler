from .utils import cache_urls

class CrawlPackage():
    def __init__(self, tocrawl={}, crawled={}):
        self.tocrawl = tocrawl
        self.crawled = crawled
        self.total_tocrawl = sum([tocrawl[host] for host in tocrawl])
        self.total_crawled = sum([crawled[host] for host in crawled])

    def add_urls_crawled(self, host, urls):
        self.total_crawled += len(urls)
        cache_urls(self.crawled, host, urls)

    def add_urls_tocrawl(self, host, urls):
        self.total_tocrawl += len(urls)
        cache_urls(self.tocrawl, host, urls)
