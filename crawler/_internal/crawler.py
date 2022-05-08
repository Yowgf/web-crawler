import json
import concurrent.futures
from datetime import datetime
import time
import unicodedata

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
from .utils import cache_urls
from .utils import is_valid_url
from .utils import CONTENT_TYPE_KEY
from .utils import DEFAULT_PROTOCOL
from .utils import SOUP_PARSER
from .utils import VALID_CONTENT_TYPE

logger = log.logger()

# Overall TODOs:
#
# - Activate page capturing using WARCIO
#
# - Limit number of work given to thread in _submit_jobs, to improve load
#   balancing.
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
            crawl_package = CrawlPackage()

            # Start with seeds
            for seed in self._seeds:
                _, host, _ = parse_url(seed)
                self._register_urls(crawl_package, host, [], [seed])
            logger.info(f"Submitting jobs for initial seeds: {crawl_package.tocrawl}")
            self._submit_jobs(crawl_package, len(self._seeds), executor,
                              results, resultsidx)

            # Go deeper
            iteration = 0
            total_num_crawled = 0
            stop = False
            while not stop:
                logger.info(f"Starting iteration number {iteration}. Results index: {resultsidx}. Number of hosts available for crawling: {len(crawl_package.tocrawl)}")

                # Make sure at least one job is submitted every iteration. This
                # is specially important in the second iteration (iteration
                # number 1).
                self._submit_jobs(crawl_package, 1, executor, results,
                                  resultsidx_inc(resultsidx))

                # Aggregate one run of crawling.
                for future in concurrent.futures.as_completed(results[resultsidx]):
                    host, crawled_urls, new_urls = future.result()
                    total_num_crawled += len(crawled_urls)
                    if total_num_crawled >= self._config.page_limit:
                        logger.info(f"Stopping due to page limit. Number of "+
                                    f"crawled pages: {total_num_crawled}. "+
                                    f"Page limit: {self._config.page_limit}")
                        stop = True
                        break

                    logger.info(f"Found {len(new_urls)} potentially new URLs")
                    self._register_urls(crawl_package, host, crawled_urls, new_urls)

                    # Go ahead and submit some jobs already, if there are
                    # any. We shouldn't wait for all threads to complete before
                    # doing this.
                    #
                    # Note that we use the next index of `results`, though. This
                    # guarantees that results[resultsidx] is a list with
                    # decreasing length.
                    if (len(results[resultsidx_inc(resultsidx)]) <
                        self._config.max_workers * 2
                    ):
                        self._submit_jobs(crawl_package, 2, executor, results,
                                          resultsidx_inc(resultsidx))

                if stop:
                    break

                results[resultsidx] = []
                resultsidx = resultsidx_inc(resultsidx)
                iteration += 1

            self._shutdown_threads(executor, results)

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

    def _shutdown_threads(self, executor, results):
        logger.info("Shutting down...")
        executor.shutdown(wait=False)
        for result in results:
            for future in result:
                future.cancel()
        logger.info("Shut down.")

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
        
        relevant_words = []
        num_relevant_words = 20
        for element in texts:
            if self._is_relevant_text(element):
                element_str = str(element)
                while "  " in element_str:
                    element_str.replace("  ", " ")
                element_str = element_str.strip()
                relevant_words.extend(element_str.split(" "))

            if len(relevant_words) >= num_relevant_words:
                break

        return " ".join(relevant_words[:20])

    def _print_debug(self, url, soup):
        debug_json_obj = {}
        debug_json_obj['URL'] = url
        if soup.title != None and soup.title.string != None:
            debug_json_obj['Title'] = soup.title.string
        else:
            debug_json_obj['Title'] = ""
        debug_json_obj['Text'] = self._find_relevant_text(soup)
        debug_json_obj['Timestamp'] = int(time.time())

        print(json.dumps(debug_json_obj, ensure_ascii=False))

    def _is_url_new(self, crawled, host, url):
        crawled_pages_in_host = crawled.get(host)
        if crawled_pages_in_host != None and url in crawled_pages_in_host:
            return False
        return True

    def _register_urls(self, crawl_package, host, crawled_urls,
                       potentially_new_urls):
        cache_urls(crawl_package.crawled, host, crawled_urls)

        new_urls = []
        for url in potentially_new_urls:
            if not self._is_url_new(crawl_package.crawled, host, url):
                # Not a new URL: skip
                continue
            new_urls.append(url)
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

    def _crawl(self, crawl_shard, http_pool):
        logger.debug(f"Received crawl shard to crawl: {crawl_shard.tocrawl}")

        crawled_normalized_urls = []

        try:
            for parent_url in crawl_shard.tocrawl:
                # Send request
                logger.debug(f"Crawling url: {parent_url}")
                try:
                    resp = http_pool.request('GET', parent_url, timeout=1.5)
                except urllib3.exceptions.MaxRetryError as e:
                    logger.debug(f"Timeout for parent url {parent_url}: {e}")
                    continue

                logger.debug('Got response: ' + str(resp))

                content_types = resp.getheaders().get(CONTENT_TYPE_KEY)
                if (content_types == None or
                    VALID_CONTENT_TYPE not in content_types
                ):
                    continue
                
                # Parse page
                soup = soup = BeautifulSoup(resp.data, SOUP_PARSER)
                
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
        except Exception as e:
            logger.error(f"Exception when crawling URL '{parent_url}': {e}",
                         exc_info=True)
            return crawl_shard.host, crawl_shard.tocrawl, crawled_normalized_urls

        return crawl_shard.host, crawl_shard.tocrawl, crawled_normalized_urls
