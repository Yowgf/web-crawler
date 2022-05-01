import json
import multiprocessing as mp # TODO
from datetime import datetime
import time

# Crawling libraries
from bs4 import BeautifulSoup
from bs4.element import Comment as bs4_comment
# warcio.capture_http.capture_http must come before the lib that calls http
# client.
from warcio.capture_http import capture_http
import urllib3
from url_normalize import url_normalize
from reppy.robots import Robots

from . import log
from .utils import get_host

logger = log.logger()

class Crawler:
    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    # Default crawl delay, in seconds
    _default_crawl_delay = 0.2

    def __init__(self, config):
        self._config = config

        self._user_agent = self._config.crawler_name

    def init(self):
        seeds_file = self._config.seeds_file
        try:
            self._seeds = open(seeds_file, "r").read().split("\n")
            logger.info(f"Got seed urls from file {seeds_file}: {self._seeds}")
        except FileNotFoundError as e:
            logger.error(f"Error reading seeds file '{seeds_file}': {e}")
            raise

        # TODO: improve efficiency

        self._http_pool = urllib3.PoolManager(headers={
            'User-Agent': self._user_agent,
        })
        self._crawled_pages = []

        # robots_cache is a map host -> parsed robots.txt file.
        self._robots_cache = {}

        # Clean output file
        open(self._config.output_pages_path, 'w')

    # run assumes that Crawler.init has already been called upon the object.
    def run(self):
        before = datetime.now()

        with capture_http(self._config.output_pages_path):
            for seed in self._seeds:
                self._crawl(seed, None, max_depth=2)

        elapsed = datetime.now() - before
        logger.info(f"Elapsed time: {elapsed}")

    def _is_relevant_text(self, soup_element):
        if soup_element.parent.name in Crawler._nontext_tags:
            return False
        if isinstance(soup_element, bs4_comment):
            return False
        if str(soup_element) == ' ' or str(soup_element) == '\n':
            return False
        return True

    # _filter_relevant_text returns a list with elements considered to be
    # relevant texts, given a list of raw texts.
    #
    # see _is_relevant_text
    def _filter_relevant_text(self, soup_elements):
        relevant_texts = []
        for element in soup_elements:
            if self._is_relevant_text(element):
                relevant_texts.append(element)
        return relevant_texts

    # _find_relevant_text is like _filter_relevant_text, but specialized to return
    # the first relevant text, instead of a list
    def _find_relevant_text(self, soup):
        texts = soup.findAll(text=True)
        
        relevant_text = ""
        for element in texts:
            if self._is_relevant_text(element):
                relevant_text = relevant_text + " " + str(element)
            if len(relevant_text) >= 20:
                break

        # Trim text
        relevant_text = relevant_text.strip()
        while "  " in relevant_text:
            relevant_text.replace("  ", " ")

        first_20words_relevant_text = str(relevant_text).split(" ")[:20]
        return " ".join(first_20words_relevant_text)

    def _print_debug(self, url, soup):
        debug_json_obj = {}
        debug_json_obj['URL'] = url
        if soup.title != None:
            debug_json_obj['Title'] = str(soup.title.string)
        else:
            debug_json_obj['Title'] = ""
        debug_json_obj['Text'] = self._find_relevant_text(soup)
        debug_json_obj['Timestamp'] = int(time.time())

        print(json.dumps(debug_json_obj))

    def _normalize(self, parent_url, url):
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

        if normalized_url in self._crawled_pages:
            return normalized_url, True, "page has already been crawled"

        logger.debug(f"Normalized url: {normalized_url}")

        return normalized_url, False, ""

    def _get_crawl_delay_from_policy(self, robots_policy):
        crawl_delay = robots_policy.delay
        if crawl_delay == None:
            crawl_delay = self._default_crawl_delay
        return crawl_delay
        

    def _get_crawl_delay(self, normalized_url):
        host = get_host(normalized_url)
        if host in self._robots_cache.keys():
            return self._get_crawl_delay_from_policy(self._robots_cache[host])

        robots_url = "http://" + get_host(normalized_url) + "/robots.txt"
        robots_resp = Robots.fetch(robots_url)
        policy = robots_resp.agent(self._user_agent)

        # Cache the info
        self._robots_cache[host] = policy

        return self._get_crawl_delay_from_policy(policy)

    def _crawl(self, parent_url, url, max_depth=10):
        # Recursion fire wall
        if max_depth == 0 or len(self._crawled_pages) >= self._config.page_limit:
            return
        if url == None:
            url = parent_url

        # Normalize URL
        normalized_url, should_skip, reason = self._normalize(parent_url, url)
        if should_skip:
            logger.debug(f"Skipping url {normalized_url}. Reason: {reason}")
            return

        # Wait a bit to avoid being blocked. Must follow policy defined in
        # <host>/robots.txt.
        crawl_delay = self._get_crawl_delay(normalized_url)
        time.sleep(crawl_delay)

        # Send request
        logger.debug(f"Crawling url: {normalized_url}")
        resp = self._http_pool.request('GET', normalized_url)
        logger.debug('Got response: ' + str(resp))

        # Register page
        self._crawled_pages.append(normalized_url)

        # Parse page
        soup = BeautifulSoup(resp.data, 'html.parser')

        if self._config.debug:
            self._print_debug(normalized_url, soup)

        # Search for child hyperlinks
        links = soup.findAll('a')
        child_urls = [link.attrs.get('href') for link in links]
        # Filter children
        child_urls = [url for url in child_urls
                      if (url != None and
                          not url.startswith("#"))
        ]
        # Crawl child hyperlinks
        for child_url in child_urls:
            self._crawl(url, child_url, max_depth=max_depth-1)
