import json
import concurrent.futures
from datetime import datetime
import time
from threading import Lock

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
    #
    # ALL THESE CONSTANTS ARE READ-ONLY.
    #

    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    # Default crawl delay, in seconds
    _default_crawl_delay = 0.2
    _default_user_agent = "simple-web-crawler/v1.0"
    _default_http_headers = {
        'User-Agent': _default_user_agent,
    }

    def __init__(self, config):
        self._config = config

    def init(self):
        seeds_file = self._config.seeds_file
        try:
            self._seeds = open(seeds_file, "r").read().split("\n")
            logger.info(f"Got seed urls from file {seeds_file}: {self._seeds}")
        except FileNotFoundError as e:
            logger.error(f"Error reading seeds file '{seeds_file}': {e}")
            raise

        self._crawled_pages = set()
        self._crawled_pages_lock = Lock()

        # robots_cache is a map host -> parsed robots.txt file.
        self._robots_cache = {}
        self._robots_cache_lock = Lock()

        # Clean output file
        open(self._config.output_pages_path, 'w')

    # run assumes that Crawler.init has already been called upon the object.
    def run(self):
        before = datetime.now()

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._config.max_workers,
        ) as executor:
            results = [[], []]
            curidx = 0

            for seed in self._seeds:
                http_pool = urllib3.PoolManager(
                    headers=Crawler._default_http_headers)
                max_depth = 10
                results[curidx].append(
                    executor.submit(
                        self._crawl, seed, seed, http_pool, max_depth))

            stop_everything = False
            while not stop_everything:
                curidx = (curidx + 1) % 2

                for future in concurrent.futures.as_completed(results[curidx]):
                    parent_url, child_urls, max_depth, stop = future.result()
                    if stop:
                        stop_everything = True
                        break

                    if max_depth == 0:
                        continue

                    for child_url in child_urls:
                        http_pool = urllib3.PoolManager(
                            headers=Crawler._default_http_headers)

                        results[curidx].append(
                            executor.submit(
                                self._crawl, parent_url, child_url, http_pool,
                                max_depth))

                results[curidx] = []

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

    # TODO: check if page has HTML content before crawling.
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
        policy = robots_resp.agent(Crawler._default_user_agent)

        # Cache the info
        self._robots_cache[host] = policy

        return self._get_crawl_delay_from_policy(policy)

    def _crawl(self, parent_url, url, http_pool, max_depth):
        with self._crawled_pages_lock:

            # Fire wall
            if len(self._crawled_pages) >= self._config.page_limit:
                # REQUEST FOR GLOBAL STOP!
                return None, None, 0, True
            if max_depth == 0:
                return None, None, 0, False
                
            # Normalize URL
            normalized_url, should_skip, reason = self._normalize(parent_url, url)
            if should_skip:
                logger.debug(f"Skipping url {normalized_url}. Reason: {reason}")
                return None, None, 0, False

            # Append earlier to make good use of the acquired lock.
            self._crawled_pages.add(normalized_url)

        # TODO: this won't work with parallel processing. Why?
        #
        # Wait a bit to avoid being blocked. Must follow policy defined in
        # <host>/robots.txt.
        with self._robots_cache_lock:
            crawl_delay = self._get_crawl_delay(normalized_url)
        time.sleep(crawl_delay)

        # Send request
        logger.debug(f"Crawling url: {normalized_url}")
        resp = http_pool.request('GET', normalized_url, timeout=1)
        logger.debug('Got response: ' + str(resp))

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

        return parent_url, child_urls, max_depth-1, False
