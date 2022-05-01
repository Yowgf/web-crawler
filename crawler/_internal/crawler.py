import json
import multiprocessing as mp # TODO
import time

# Crawling libraries
from bs4 import BeautifulSoup
from bs4.element import Comment as bs4_comment
# import requests
import urllib3
from url_normalize import url_normalize
from reppy.robots import Robots
from warcio.capture_http import capture_http

from . import log
from .utils import get_host

logger = log.logger()

class Crawler:
    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    def __init__(self, config):
        self._config = config

    def init(self):
        seeds_file = self._config.seeds_file
        try:
            self.seeds = open(seeds_file, "r").read().split("\n")
            logger.info(f"Got seed urls from file {seeds_file}: {self.seeds}")
        except FileNotFoundError as e:
            logger.error(f"error reading seeds file '{seeds_file}': {e}")
            raise

        self.http_pool = urllib3.PoolManager()
        self.crawled_pages = []

    def is_relevant_text(self, soup_element):
        if soup_element.parent.name in Crawler._nontext_tags:
            return False
        if isinstance(soup_element, bs4_comment):
            return False
        if str(soup_element) == ' ' or str(soup_element) == '\n':
            return False
        return True

    # filter_relevant_text returns a list with elements considered to be
    # relevant texts, given a list of raw texts.
    #
    # see is_relevant_text
    def filter_relevant_text(self, soup_elements):
        relevant_texts = []
        for element in soup_elements:
            if self.is_relevant_text(element):
                relevant_texts.append(element)
        return relevant_texts

    # find_relevant_text is like filter_relevant_text, but specialized to return
    # the first relevant text, instead of a list
    def find_relevant_text(self, soup):
        texts = soup.findAll(text=True)
        
        relevant_text = ""
        for element in texts:
            if self.is_relevant_text(element):
                relevant_text = relevant_text + " " + str(element)
            if len(relevant_text) >= 20:
                break

        # Trim text
        relevant_text = relevant_text.strip()
        while "  " in relevant_text:
            relevant_text.replace("  ", " ")

        first_20words_relevant_text = str(relevant_text).split(" ")[:20]
        return " ".join(first_20words_relevant_text)

    def print_debug(self, url, soup):
        debug_json_obj = {}
        debug_json_obj['URL'] = url
        if soup.title != None:
            debug_json_obj['Title'] = str(soup.title.string)
        else:
            debug_json_obj['Title'] = ""
        debug_json_obj['Text'] = self.find_relevant_text(soup)
        debug_json_obj['Timestamp'] = int(time.time())

        print(json.dumps(debug_json_obj))

    def normalize(self, parent_url, url):
        logger.debug(f"Normalizing url {url} with parent {parent_url}")

        normalized_url = None

        if url.startswith("/"):
            normalized_url = "http://" + get_host(parent_url) + url
        elif url.startswith("./"):
            normalized_url = "http://" + get_host(parent_url) + url[1:]
        elif url.startswith("../"):
            normalized_url = "http://" + get_host(parent_url, 2) + url[2:]
        else:
            normalized_url = url

        normalized_url = url_normalize(normalized_url)

        if normalized_url in self.crawled_pages:
            return normalized_url, True, "page has already been crawled"

        logger.debug(f"Normalized url: {normalized_url}")

        return normalized_url, False, ""

    def crawl(self, parent_url, url, max_depth=10):
        # Fire wall
        if max_depth == 0 or len(self.crawled_pages) >= self._config.page_limit:
            return
        if url == None:
            url = parent_url

        # Normalize URL
        url, should_skip, reason = self.normalize(parent_url, url)
        if should_skip:
            logger.debug(f"Skipping url {url}. Reason: {reason}")
            return

        # Wait a bit to avoid being blocked
        time.sleep(0.2)

        # Send request
        logger.debug(f"Crawling url: {url}")
        resp = self.http_pool.request('GET', url)
        self.crawled_pages.append(url)
        logger.debug('Got response: ' + str(resp))
        
        soup = BeautifulSoup(resp.data, 'html.parser')

        if self._config.debug:
            self.print_debug(url, soup)

        # Search for child hyperlinks
        links = soup.findAll('a')
        child_urls = [link.attrs.get('href') for link in links]
        # Filter children
        child_urls = [url for url in child_urls
                      if url != None and
                      not url.startswith("#")
        ]
        # Crawl child hyperlinks
        for child_url in child_urls:
            self.crawl(url, child_url, max_depth=max_depth-1)

    # run assumes that Crawler.init has already been called upon the object.
    def run(self):
        with capture_http('crawled_pages.gz'):
            for seed in self.seeds:
                self.crawl(seed, None, max_depth=2)
