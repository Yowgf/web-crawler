# Standard libraries
import concurrent.futures
import json
from datetime import datetime
import glob
import gzip
import shutil
import socket
import os
from threading import get_native_id
import time
import unicodedata

# Crawling libraries
#
# warcio.capture_http.capture_http must come before the lib that calls http
# client.
from warcio.capture_http import capture_http
from warcio.recompressor import Recompressor
# HTML parsing
from bs4 import BeautifulSoup
from bs4.element import Comment as bs4_comment
# HTTP communication
import urllib3
import requests
# URL normalization
from url_normalize import url_normalize
# Robots.txt parsing
from reppy.robots import Robots

from . import log
from .config import Config
from .crawl_package import CrawlPackage
from .crawl_shard import CrawlShard
from .robots import FakeRobotsPolicy
from .utils import is_valid_url
from .utils import len_two_dicts_entry
from .utils import get_robots_url
from .utils import parse_url
from .utils import suppress_output
from .utils import CONTENT_TYPE_KEY
from .utils import HTTP_SCHEME, HTTPS_SCHEME
from .utils import MAX_URL_LENGTH
from .utils import SOUP_PARSER
from .utils import VALID_CONTENT_TYPE

logger = log.logger()

# TODOs:
#
# - 
################################################################################

class Crawler:
    #
    # ALL THESE CONSTANTS ARE READ-ONLY.
    #

    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    _default_user_agent = "simple-web-crawler/v1.0"

    _default_http_headers = {
        'User-Agent': _default_user_agent,
    }

    # CRAWLING LIMITS
    #
    # Each thread can only work with this many urls at a time. This avoids
    # having a thread that takes to long to complete its task.
    _max_urls_per_job = 25
    #
    _max_workers = 32
    #
    _max_njobs = _max_workers * 10
    #
    _max_urls_per_host = 1000
    #
    _page_limit_overflow_allowed = 5000
    #
    _default_timeout = 2 # seconds

    # Crawl delay constants, in seconds
    _default_crawl_delay = 0.2
    _max_crawl_delay = 2

    def __init__(self, config):
        self._config = config

        # _robots_cache is a map host -> parsed robots.txt file.
        self._robots_cache = {}

        # _active_hosts is a set used to indicate if a host is actively being
        # crawled by some thread.
        self._active_hosts = set()

        # _unreachable_hosts is a set of hosts for which DNS probing failed. We
        # don't want to keep retrying on these.
        self._unreachable_hosts = set()

        # _crawl_package is the main global source of information of which pages
        # have already been crawled, and which pages have yet to be crawled.
        self._crawl_package = CrawlPackage()

        self._default_robots_policy = FakeRobotsPolicy(self._max_crawl_delay)

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

        self._cleanup_output_files()

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
            results = []

            # Start with seeds
            tocrawl = self._get_tocrawl(self._seeds)
            self._register_urls(self._crawl_package, {}, tocrawl)
            logger.info(f"Submitting jobs for initial seeds: "+
                        f"{self._crawl_package.tocrawl}")
            self._submit_jobs(self._crawl_package, executor, results)

            # Go deeper
            iteration = 0
            while True:
                logger.info(
                    f"Starting iteration number {iteration}. "+
                    f"Number of results available: {len(results)}. "+
                    f"Number of crawled pages so far: "+
                    f"{self._crawl_package.total_crawled}. Number of pages "+
                    f"available for crawling: {self._crawl_package.total_tocrawl}."
                )
                if len(results) == 0:
                    logger.info("Stopping crawling: no jobs left.")
                    break
                if self._crawl_package.total_crawled >= self._config.page_limit:
                    logger.info(f"Stopping due to page limit. Number of "+
                                f"crawled pages: "+
                                f"{self._crawl_package.total_crawled}. "+
                                f"Page limit: {self._config.page_limit}")
                    break

                completed, not_completed = concurrent.futures.wait(
                    results,
                    timeout=3.1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                results = list(not_completed)
                
                # TODO: Processing completed results takes a long time... So we
                # scale some job submissions in the middle, so that the master
                # thread doesn't get stuck here.
                for future in completed:
                    self._process_complete_future(self._crawl_package, future)

                self._submit_jobs(self._crawl_package, executor, results)

                iteration += 1

            self._shutdown_threads(executor, results)

        self._aggregate_pages()

    def _submit_jobs(self, crawl_package, executor, results):
        if crawl_package.total_tocrawl == 0:
            return

        tocrawl = {}
        crawl_delays = {}
        for host in list(crawl_package.tocrawl.keys()):
            # Very important to have this maximum jobs limit.
            if len(results) + len(tocrawl) >= self._max_njobs:
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
                new_urls = []
                for _ in range(self._max_urls_per_job):
                    if (crawl_package.tocrawl[host] == None or
                        len(crawl_package.tocrawl[host]) == 0
                    ):
                        crawl_package.remove_tocrawl(host)
                        break
                    new_url = crawl_package.pop_tocrawl(host)
                    new_urls.append(new_url)
                if len(new_urls) > 0:
                    tocrawl[host] = new_urls
                    self._active_hosts.add(host)

        for host in tocrawl:
            crawl_shard = CrawlShard(host, crawl_delays[host], tocrawl[host])

            # Submit job with dedicated HTTP pool
            http_pool = urllib3.PoolManager(headers=Crawler._default_http_headers)
            results.append(executor.submit(self._crawl, crawl_shard, http_pool))

    # _process_complete_future takes a completed crawling task and processes
    # it. It removes the crawled host from the list of active hosts, and
    # registers the new crawled URLs to be crawled later on.
    #
    # The function returns True if the master thread should stop, or False
    # otherwise.
    def _process_complete_future(self, crawl_package, future):
        try:
            host, crawled_urls, new_urls = future.result()
        except Exception as e:
            logger.error(f"Error retrieving future result: {e}")
            self._active_hosts.remove(host)
            return

        self._active_hosts.remove(host)

        tocrawl = self._get_tocrawl(new_urls)

        logger.info(f"Found {len(new_urls)} potentially new URLs")
        self._register_urls(crawl_package, {host: crawled_urls}, tocrawl)

    # _get_tocrawl separates the urls into different hosts to form a dict
    # host->url.
    def _get_tocrawl(self, urls):
        tocrawl = {}
        for url in urls:
            _, host, _ = parse_url(url)
            if not tocrawl.get(host):
                tocrawl[host] = []
            tocrawl[host].append(url)
        return tocrawl

    def _register_urls(self, crawl_package, recently_crawled,
                       potentially_new_tocrawl):
        for host in recently_crawled:
            crawl_package.add_urls_crawled(host, recently_crawled[host])

        if (crawl_package.total_tocrawl + crawl_package.total_crawled >=
            self._config.page_limit + self._page_limit_overflow_allowed
        ):
            return

        for host in potentially_new_tocrawl:
            # Check if site can be reached at all. If not, don't bother with
            # it. This can save many seconds of our life.
            if self._is_host_unreachable(host):
                continue

            if (len_two_dicts_entry(crawl_package.crawled,
                                    crawl_package.tocrawl, host) > 
                self._max_urls_per_host
            ):
                # Skip this host
                continue

            # Register the robots.txt policy
            self._register_robot_policy(host)
            robots_policy = self._robots_cache.get(host)

            potentially_new_urls = potentially_new_tocrawl[host]
            new_urls = []
            for url in potentially_new_urls:
                if not self._is_url_new(crawl_package.crawled, host, url):
                    # Not a new URL: skip
                    continue
                elif robots_policy != None and not robots_policy.allowed(url):
                    continue
                new_urls.append(url)
            crawl_package.add_urls_tocrawl(host, new_urls)

    def _shutdown_threads(self, executor, results):
        logger.info("Shutting down...")
        executor.shutdown(wait=False)
        for future in results:
            future.cancel()
        logger.info("Shut down.")

    def _aggregate_pages(self):
        output_fpath = self._config.output_pages_path
        temp_fpath = "web-crawler-temp.gz"
        # Empty output files
        open(output_fpath, 'w')
        open(temp_fpath, 'w')

        for input_fpath in glob.glob("*_" + self._config.output_pages_path):
            with gzip.open(input_fpath) as fin:
                with gzip.open(temp_fpath, 'ab') as fout:
                    shutil.copyfileobj(fin, fout)

        # Fix any compression errors
        rc = Recompressor(temp_fpath, output_fpath, verbose=False)
        with suppress_output():
            rc.recompress()

        os.remove(temp_fpath)
        self._cleanup_output_files()

    def _cleanup_output_files(self):
        # Clean output files
        for fpath in glob.glob("*_" + self._config.output_pages_path):
            os.remove(fpath)

    def _is_host_unreachable(self, host):
        if host in self._unreachable_hosts:
            return True
        try:
            socket.getaddrinfo(host, 80)
        except socket.gaierror:
            self._unreachable_hosts.add(host)
            return True
        except:
            try:
                socket.getaddrinfo(host, 443)
            except socket.gaierror:
                self._unreachable_hosts.add(host)
                return True
            except:
                return False
        return False

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

    def _use_default_robot_policy(self, host):
        self._robots_cache[host] = self._default_robots_policy

    def _register_robot_policy(self, host):
        if self._robots_cache.get(host) != None:
            return True

        # TODO: be more careful with which types of exceptions to catch.
        #
        # Try to get robots.txt policy using both HTTP and HTTPS.
        requests_headers = headers={'timeout': str(self._default_timeout)}
        try:
            robots_url = get_robots_url(HTTP_SCHEME, host)
            robots_resp = Robots.fetch(robots_url, headers=requests_headers)
        except requests.Timeout as e:
            logger.info(f"Timeout while fetching robots page "+
                        f"'{robots_url}' with {HTTP_SCHEME}: {e}")
            try:
                robots_url = get_robots_url(HTTPS_SCHEME, host)
                robots_resp = Robots.fetch(robots_url, headers=requests_headers)
            except requests.Timeout as e:
                logger.info(f"Timeout while fetching robots page "+
                            f"'{robots_url}' with {HTTPS_SCHEME}: {e}")
                # Indicate that something went wrong by returning None. We can
                # try again later.
                return None
            except Exception as e:
                logger.info(f"Caught exception while fetching robots page "+
                            f"'{robots_url}': {e}. Using default robot policy.")
                self._use_default_robot_policy(host)
                return True
        except Exception as e:
            logger.info(f"Caught exception while fetching robots page "+
                         f"'{robots_url}': {e}. Using default robot policy.")
            self._use_default_robot_policy(host)
            return True

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
        elif href.startswith('javascript:'):
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
        if len(normalized_url) > MAX_URL_LENGTH:
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

    def _crawl_url(self, tid, http_pool, url):
        logger.debug(f"({tid}) Crawling url: {url}")
        try:
            resp = http_pool.request('GET', url, timeout=self._default_timeout)
        except urllib3.exceptions.MaxRetryError as e:
            logger.info(f"({tid}) Timeout for parent url '{url}': {e}")
            return None
        except Exception as e:
            logger.info(f"({tid}) Error when requesting for '{url}': {e}")
            return None

        logger.debug(f'({tid}) Got response status: {resp.status}')

        content_types = resp.getheaders().get(CONTENT_TYPE_KEY)
        if (content_types == None or
            VALID_CONTENT_TYPE not in content_types
        ):
            return None

        soup = soup = BeautifulSoup(resp.data, SOUP_PARSER)
        
        if self._config.debug:
            self._print_debug(url, soup)

        # Transform hrefs into normalized URLs based on parent
        hrefs = self._find_hrefs(soup)
        normalized_child_urls = self._get_href_normalized_urls(url, hrefs)

        return normalized_child_urls

    def _crawl(self, crawl_shard, http_pool):
        tid = get_native_id()
        logger.debug(f"({tid}) Started crawling. Received shard: "+
                     f"{crawl_shard.tocrawl}")

        crawled_parent_urls = []
        crawled_child_urls = []

        out_fpath = str(tid) + "_" + self._config.output_pages_path
        with capture_http(out_fpath):
            logger.debug(f"({tid}) Capturing pages to '{out_fpath}'")

            for parent_url in crawl_shard.tocrawl:
                child_urls = self._crawl_url(tid, http_pool, parent_url)

                if child_urls == None:
                    continue

                crawled_parent_urls.append(parent_url)
                crawled_child_urls.extend(child_urls)
                
                # Wait a bit to avoid being blocked. Must follow policy defined
                # in <host>/robots.txt.
                #
                # For this to work, it is assumed that the shard the thread
                # received contains only URLs for a single host.
                logger.debug(f"({tid}) Sleeping crawl delay of "+
                             f"{crawl_shard.delay} seconds")
                time.sleep(crawl_shard.delay)

        logger.debug(f"({tid}) Finished crawling.")

        return crawl_shard.host, crawled_parent_urls, crawled_child_urls
