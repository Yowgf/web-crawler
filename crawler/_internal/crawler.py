import json
import concurrent.futures
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
from .config import Config
from .crawl_package import CrawlPackage
from .crawl_shard import CrawlShard
from .utils import parse_url
from .utils import cache_url
from .utils import cache_urls
#from .utils import cache_url_packets
from .utils import DEFAULT_PROTOCOL
from .utils import is_valid_url

logger = log.logger()

# Overall TODOs:
#
# - Use right locale based on website's info
################################################################################

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

        # robots_cache is a map host -> parsed robots.txt file.
        self._robots_cache = {}

        # Clean output file
        open(self._config.output_pages_path, 'w')

    # run assumes that Crawler.init has already been called upon the object.
    def run(self):
        before = datetime.now()

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._config.max_workers,
        ) as executor:
            results = [[], []]
            resultsidx = 0
            def resultsidx_inc(resultsidx):
                return (resultsidx + 1) % len(results)
            # TODO: tune the max_depth parameter and make it work.
            max_depth = 10
            crawl_package = CrawlPackage()

            # Start with seeds
            parsed_seeds = {}
            for seed in self._seeds:
                _, host, _ = parse_url(seed)
                parsed_seeds[host] = [seed]
            self._register_urls(crawl_package, parsed_seeds)
            logger.info(f"Submitting jobs for initial seeds {parsed_seeds}")
            self._submit_jobs(crawl_package, len(self._seeds), executor,
                              results, resultsidx)

            # Go deeper
            while True:
                # Aggregate one run of crawling.
                crawled_aggr = {}
                for future in concurrent.futures.as_completed(results[resultsidx]):
                    host, crawled_normalized_urls = future.result()
                    cache_urls(crawled_aggr, host, crawled_normalized_urls)

                    # Go ahead and submit some jobs already, if there are
                    # any. We shouldn't wait for all threads to complete before
                    # doing this.
                    #
                    # Note that we use the next index of `results`, though. This
                    # guarantees that results[resultsidx] is a list with
                    # decreasing length.
                    self._submit_jobs(crawl_package, 2, executor, results,
                                      resultsidx_inc(resultsidx))

                # TODO: stop if necessary
                if True:
                    break

                self._register_urls(crawl_package, crawled_aggr)

                results[resultsidx] = []
                resultsidx = resultsidx_inc(resultsidx)

        elapsed = datetime.now() - before
        logger.info(f"Elapsed time: {elapsed}")

    def _submit_jobs(self, crawl_package, max_njobs, executor, results,
                     resultsidx):
        for _ in range(max_njobs):
            if len(crawl_package.tocrawl) == 0:
                return

            host, tocrawl = crawl_package.tocrawl.popitem()
            crawl_delay = self._get_crawl_delay(host)
            crawl_shard = CrawlShard(host, crawl_delay, tocrawl)

            # Submit job with dedicated HTTP pool
            http_pool = urllib3.PoolManager(headers=Crawler._default_http_headers)
            results[resultsidx].append(executor.submit(self._crawl, crawl_shard,
                                                       http_pool))

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

    def _is_url_new(self, crawled, host, url):
        crawled_pages_in_host = crawled.get(host)
        if crawled_pages_in_host != None and url in crawled_pages_in_host:
            return False
        return True

    def _register_urls(self, crawl_package, crawled_aggr):
        logger.debug(f"Registering URLs for aggregated pages: {crawled_aggr}")
        for host in crawled_aggr:
            new_urls = []
            for url in crawled_aggr[host]:
                if not self._is_url_new(crawl_package.crawled, host, url):
                    # Oops, not a new URL. Skip
                    continue
                new_urls.append(url)
            cache_urls(crawl_package.crawled, host, new_urls)
            cache_urls(crawl_package.tocrawl, host, new_urls)

    def _get_crawl_delay_from_policy(self, robots_policy):
        crawl_delay = robots_policy.delay
        if crawl_delay == None:
            crawl_delay = self._default_crawl_delay
        return crawl_delay

    def _get_crawl_delay(self, host):
        if self._robots_cache.get(host) != None:
            return self._get_crawl_delay_from_policy(self._robots_cache[host])

        robots_url = DEFAULT_PROTOCOL + "://" + host + "/robots.txt"
        robots_resp = Robots.fetch(robots_url)
        policy = robots_resp.agent(Crawler._default_user_agent)

        # Cache the info
        self._robots_cache[host] = policy

        return self._get_crawl_delay_from_policy(policy)

    def _find_hrefs(self, soup):
        links = soup.findAll('a')
        hrefs = [link.attrs.get('href') for link in links]
        return hrefs

    # TODO: check if page has HTML content before crawling.
    #
    # _get_href_normalized_url transforms an href into a normalized URL,
    # according to its parent URL. This also works in favor of avoiding page
    # revisiting.
    #
    # If anything is wrong, the function returns a None value.
    def _get_href_normalized_url(self, parent_url, href):
        logger.debug(f"Normalizing href {href} with parent {parent_url}")

        # Firewall
        if href == None or href.startswith('#'):
            return None

        normalized_url = None

        protocol, host, path_list = parse_url(parent_url)

        if href.startswith("/"):
            normalized_url = protocol + "://" + host + href
        elif href.startswith("./"):
            normalized_url = protocol + "://" + host + "/".join(path_list) + href[1:]
        elif href.startswith("../"):
            normalized_url = protocol + "://" + host + "/".join(path_list[:-1]) + href[2:]
        else:
            if not is_valid_url(href):
                # href does not represent a valid URL
                logger.debug(f"href {href} is invalid URL")
                return None
            normalized_url = href

        # Final normalizing step using library function
        normalized_url = url_normalize(normalized_url)

        logger.debug(f"Normalized url: {normalized_url}")

        return normalized_url

    def _get_href_normalized_urls(self, parent_url, hrefs):
        normalized_urls = []
        for href in hrefs:
            normalized_url = self._get_href_normalized_url(parent_url, href)
            if normalized_url != None:
                normalized_urls.append(normalized_url)
        return normalized_urls

    # TODO: allow each thread to expand into the same host as deep as it can,
    # with predefined limit.
    def _crawl(self, crawl_shard, http_pool):
        logger.debug(f"Received crawl shard to crawl: {crawl_shard.tocrawl}")

        crawled_normalized_urls = []

        for parent_url in crawl_shard.tocrawl:
            # Send request
            logger.debug(f"Crawling url: {parent_url}")
            # TODO: how should we treat timeouts?
            resp = http_pool.request('GET', parent_url, timeout=1)
            logger.debug('Got response: ' + str(resp))
            
            # Parse page
            soup = BeautifulSoup(resp.data, 'html.parser')
            
            if self._config.debug:
                self._print_debug(parent_url, soup)

            # Transform hrefs into normalized URLs based on parent
            hrefs = self._find_hrefs(soup)
            normalized_child_urls = self._get_href_normalized_urls(parent_url,
                                                                   hrefs)
            crawled_normalized_urls.extend(normalized_child_urls)

            # Wait a bit to avoid being blocked. Must follow policy defined in
            # <host>/robots.txt.
            #
            # For this to work, it is assumed that the crawling package the thread
            # received only contains URLs that refer to one host.
            time.sleep(crawl_shard.delay)
            
        return crawl_shard.host, crawled_normalized_urls
