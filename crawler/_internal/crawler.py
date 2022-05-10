import json
import concurrent.futures
from datetime import datetime
from threading import get_native_id
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
import requests
from reppy.robots import Robots

from . import log
from .config import Config
from .crawl_package import CrawlPackage
from .crawl_shard import CrawlShard
from .utils import is_valid_url
from .utils import parse_url
from .utils import CONTENT_TYPE_KEY
from .utils import DEFAULT_PROTOCOL
from .utils import SOUP_PARSER
from .utils import VALID_CONTENT_TYPE

logger = log.logger()

# Overall TODOs:
#
# - Activate page capturing using WARCIO
#
# - Limit number of pages to 2500 per host
################################################################################

class Crawler:
    #
    # ALL THESE CONSTANTS ARE READ-ONLY.
    #

    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    # Crawl delay constants, in seconds
    _default_crawl_delay = 0.2
    _max_crawl_delay = 2

    _default_user_agent = "simple-web-crawler/v1.0"

    _default_http_headers = {
        'User-Agent': _default_user_agent,
    }

    # Each thread can only work with this many urls at a time. This avoids
    # having a thread that takes to long to complete its task.
    _max_urls_per_job = 25

    _max_workers = 32
    _max_njobs = _max_workers * 10

    def __init__(self, config):
        # _robots_cache is a map host -> parsed robots.txt file.
        self._robots_cache = {}

        # _active_hosts is a set used to indicate if a host is actively being
        # crawled by some thread.
        self._active_hosts = set()

        # _crawl_package is the main global source of information of which pages
        # have already been crawled, and which pages have yet to be crawled.
        self._crawl_package = CrawlPackage()

        self._config = config

    # init contains initialization procedures that may throw an exception or
    # otherwise fail.
    def init(self):
        seeds_file = self._config.seeds_file
        try:
            self._seeds = open(seeds_file, "r").read().strip().split("\n")
            logger.info(f"Got seed urls from file {seeds_file}: {self._seeds}")
        except FileNotFoundError as e:
            logger.error(f"Error reading seeds file '{seeds_file}': {e}")
            raise

        # Clean main output file
        open(self._config.output_pages_path, 'w')

    def run_timed(self):
        before = datetime.now()

        crawled_pages = self.run()

        elapsed = datetime.now() - before
        logger.info(f"Elapsed time: {elapsed}")

        return (f"{self._crawl_package.total_crawled} pages were crawled. "+
                f"Elapsed time: {elapsed}")

    # run assumes that Crawler.init has already been called upon the object.
    def run(self):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers,
        ) as executor:
            results = [[], []]
            resultsidx = 0
            def resultsidx_inc(resultsidx):
                return (resultsidx + 1) % len(results)

            # Start with seeds
            for seed in self._seeds:
                _, host, _ = parse_url(seed)
                self._register_urls(self._crawl_package, host, [], [seed])
            logger.info(f"Submitting jobs for initial seeds: "+
                        f"{self._crawl_package.tocrawl}")
            self._submit_jobs(self._crawl_package, executor, results,
                              resultsidx)

            # Go deeper
            iteration = 0
            while True:
                logger.info(
                    f"Starting iteration number {iteration}. "+
                    f"Number of results available: {len(results[resultsidx])}. "+
                    f"Number of crawled pages so far: "+
                    f"{self._crawl_package.total_crawled}. Number of pages "+
                    f"available for crawling: {self._crawl_package.total_tocrawl}."
                )

                if self._crawl_package.total_crawled >= self._config.page_limit:
                    logger.info(f"Stopping due to page limit. Number of "+
                                f"crawled pages: "+
                                f"{self._crawl_package.total_crawled}. "+
                                f"Page limit: {self._config.page_limit}")
                    break

                # Note that we use the next index of `results`. This guarantees
                # that the current results[resultsidx] is a list with decreasing
                # length.
                self._submit_jobs(self._crawl_package, executor, results,
                                  resultsidx_inc(resultsidx))

                # Aggregate one run of crawling.
                while len(results[resultsidx]) > 0:
                    logger.debug(f"In master loop. Still "+
                                 f"{len(results[resultsidx])} results left to "+
                                 f"wait for.")

                    self._submit_jobs(self._crawl_package, executor, results,
                                      resultsidx_inc(resultsidx))

                    completed, not_completed = concurrent.futures.wait(
                        results[resultsidx],
                        timeout=1,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    results[resultsidx] = list(not_completed)

                    for future in completed:
                        self._process_complete_future(self._crawl_package, future)

                resultsidx = resultsidx_inc(resultsidx)
                iteration += 1

            self._shutdown_threads(executor, results)

    def _register_urls(self, crawl_package, host, crawled_urls,
                       potentially_new_urls):
        crawl_package.add_urls_crawled(host, crawled_urls)

        # Register the robots.txt policy
        self._register_robot_policy(host)
        robots_policy = self._robots_cache.get(host)

        # Arbitrarily chosen threshold of 1000
        if (crawl_package.total_tocrawl + crawl_package.total_crawled <
            self._config.page_limit + 1000
        ):
            new_urls = []
            for url in potentially_new_urls:
                if not self._is_url_new(crawl_package.crawled, host, url):
                    # Not a new URL: skip
                    continue
                elif robots_policy != None and not robots_policy.allowed(url):
                    continue
                new_urls.append(url)
            crawl_package.add_urls_tocrawl(host, new_urls)

    def _submit_jobs(self, crawl_package, executor, results,
                     resultsidx):
        if crawl_package.total_tocrawl == 0:
            return

        tocrawl = {}
        crawl_delays = {}
        for host in list(crawl_package.tocrawl.keys()):
            if len(results[resultsidx]) >= self._max_njobs:
                break

            # This condition is what ensures that we will not act over the same
            # host in two different threads!!!
            if host not in self._active_hosts:
                # Get crawling delay according to robots policy
                crawl_delay = self._get_crawl_delay(host)
                if crawl_delay == None:
                    continue
                crawl_delays[host] = crawl_delay

                # Add to list of urls to crawl
                tocrawl[host] = []
                self._active_hosts.add(host)
                for _ in range(self._max_urls_per_job):
                    if (crawl_package.tocrawl[host] == None or
                        len(crawl_package.tocrawl[host]) == 0
                    ):
                        crawl_package.tocrawl.pop(host)
                        break
                    new_url = crawl_package.tocrawl[host].pop()
                    tocrawl[host].append(new_url)
                continue

        for host in tocrawl:
            crawl_shard = CrawlShard(host, crawl_delays[host], tocrawl[host])

            # Submit job with dedicated HTTP pool
            http_pool = urllib3.PoolManager(headers=Crawler._default_http_headers)
            results[resultsidx].append(executor.submit(self._crawl, crawl_shard,
                                                       http_pool))

    # _process_complete_future returns True if the master thread should stop, or
    # False otherwise.
    def _process_complete_future(self, crawl_package, future):
        try:
            host, crawled_urls, new_urls = future.result()
        except Exception as e:
            logger.error(f"Error retrieving future result: {e}")
            return
        
        self._active_hosts.remove(host)
            
        logger.info(f"Found {len(new_urls)} potentially new URLs")
        self._register_urls(crawl_package, host, crawled_urls, new_urls)
            
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

    # _find_relevant_text a string with the first 20 relevant words of text in
    # the given BeautifulSoup object.
    def _find_relevant_text(self, soup, num_relevant_words=20):
        texts = soup.findAll(text=True)
        
        relevant_words = []
        for element in texts:
            if self._is_relevant_text(element):
                element_str = str(element)
                # Strip new element's string of extra spaces.
                while "  " in element_str:
                    element_str = element_str.replace("  ", " ")
                element_str = element_str.strip()

                # Add new words
                relevant_words.extend(element_str.split(" "))

            if len(relevant_words) >= num_relevant_words:
                break

        return " ".join(relevant_words[:num_relevant_words])

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

    def _get_crawl_delay_from_policy(self, robots_policy):
        crawl_delay = robots_policy.delay
        if crawl_delay == None or crawl_delay > self._max_crawl_delay:
            crawl_delay = self._default_crawl_delay
        return crawl_delay

    def _register_robot_policy(self, host):
        if self._robots_cache.get(host) != None:
            return True

        robots_url = DEFAULT_PROTOCOL + "://" + host + "/robots.txt"
        try:
            robots_resp = Robots.fetch(robots_url)
        except requests.Timeout as e:
            logger.info(f"Timeout while fetching robots page "+
                         f"'{robots_url}': {e}", exc_info=True)
            # Indicate that something went wrong by returning None.
            return None
        policy = robots_resp.agent(Crawler._default_user_agent)

        # Cache the info
        self._robots_cache[host] = policy

        return True

    def _get_crawl_delay(self, host):
        success = self._register_robot_policy(host)
        if success != True:
            return None

        return self._get_crawl_delay_from_policy(self._robots_cache[host])

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
        # This is super verbose. Leave uncommented unless suspicious of a
        # problem with this function.
        #
        # logger.debug(f"Normalizing href {href} with parent {parent_url}")

        # Firewall
        if href == None or href.startswith('#'):
            return None

        normalized_url = None

        protocol, host, path_list = parse_url(parent_url)

        if href == ' ':
            return None
        elif href == 'javascript:void(0)':
            return None
        elif href.startswith("/"):
            normalized_url = protocol + "://" + host + href
        elif href.startswith("./"):
            normalized_url = protocol + "://" + host + "/".join(path_list) + href[1:]
        elif href.startswith("../"):
            normalized_url = protocol + "://" + host + "/".join(path_list[:-1]) + href[2:]
        else:
            if not is_valid_url(href):
                return None
            normalized_url = href

        # Final normalizing step using library function
        normalized_url = url_normalize(normalized_url)
        if len(normalized_url) > 2048:
            return None

        # This is super verbose. Leave uncommented unless suspicious of a
        # problem with this function.
        #
        # logger.debug(f"Normalized url: {normalized_url}")

        return normalized_url

    def _get_href_normalized_urls(self, parent_url, hrefs):
        normalized_urls = []
        for href in hrefs:
            normalized_url = self._get_href_normalized_url(parent_url, href)

            if normalized_url == None:
                logger.debug(f"Unable to normalize href '{href}' into valid URL")
            else:
                normalized_urls.append(normalized_url)
        return normalized_urls

    def _crawl(self, crawl_shard, http_pool):
        logger.debug(f"({get_native_id()}) Started crawling. Received shard: "+
                     f"{crawl_shard.tocrawl}")

        crawled_parent_urls = []
        crawled_child_urls = []

        for parent_url in crawl_shard.tocrawl:
            # Send request
            logger.debug(f"({get_native_id()}) Crawling url: {parent_url}")
            try:
                resp = http_pool.request('GET', parent_url, timeout=1.5)
            except urllib3.exceptions.MaxRetryError as e:
                logger.info(f"({get_native_id()}) Timeout for parent url "+
                            f"'{parent_url}': {e}")
                continue
            
            logger.debug(f'({get_native_id()}) Got response status: '+
                         f'{resp.status}')

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
            normalized_child_urls = self._get_href_normalized_urls(
                parent_url, hrefs)
            # Register our crawling success.
            crawled_parent_urls.append(parent_url)
            crawled_child_urls.extend(normalized_child_urls)
            
            # Wait a bit to avoid being blocked. Must follow policy defined
            # in <host>/robots.txt.
            #
            # For this to work, it is assumed that the shard the thread
            # received contains only URLs for a single host.
            logger.debug(f"({get_native_id()}) Sleeping crawl delay of "+
                         f"{crawl_shard.delay} seconds")
            time.sleep(crawl_shard.delay)

        logger.debug(f"({get_native_id()}) Finished crawling.")

        return crawl_shard.host, crawled_parent_urls, crawled_child_urls
