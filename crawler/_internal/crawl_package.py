from .utils import cache_urls

class CrawlPackage():
    def __init__(self, tocrawl={}, crawled={}):
        self.tocrawl = tocrawl
        self.crawled = crawled
        self.total_tocrawl = sum([tocrawl[host] for host in tocrawl])
        self.total_crawled = sum([crawled[host] for host in crawled])

    def add_urls_crawled(self, host, urls, max_crawled=None):
        self.total_crawled += len(urls)
        if max_crawled != None:
            cache_urls(self.crawled, host, urls[:max_crawled])
        else:
            cache_urls(self.crawled, host, urls)

    def add_urls_tocrawl(self, host, urls, max_tocrawl=None):
        self.total_tocrawl += len(urls)
        if max_tocrawl != None:
            cache_urls(self.tocrawl, host, urls[:max_tocrawl])
        else:
            cache_urls(self.tocrawl, host, urls)

    def remove_crawled(self, host):
        if self.crawled.get(host):
            self.total_crawled -= len(self.crawled[host])
            self.crawled.pop(host)

    def remove_tocrawl(self, host):
        if self.tocrawl.get(host):
            self.total_tocrawl -= len(self.tocrawl[host])
            self.tocrawl.pop(host)

    def pop_crawled(self, host):
        self.total_crawled -= 1
        return self.crawled[host].pop()

    def pop_tocrawl(self, host):
        self.total_tocrawl -= 1
        return self.tocrawl[host].pop()
