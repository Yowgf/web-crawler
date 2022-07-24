# Standard libraries
import concurrent.futures
from copy import deepcopy
import json
from datetime import datetime
import glob
import gzip
import random
import shutil
import os
from threading import get_ident
from threading import Lock
import time

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
from .robots import FakeRobotsPolicy
from .utils import is_valid_url
from .utils import len_two_dicts_entry
from .utils import fragile_with
from .utils import get_robots_url
from .utils import parse_url
from .utils import suppress_output
from .utils import CONTENT_TYPE_KEY
from .utils import HTTP_SCHEME, HTTPS_SCHEME
from .utils import MAX_URL_LENGTH
from .utils import SOUP_PARSER
from .utils import VALID_CONTENT_TYPE

logger = log.logger()

class Crawler:
    #
    # ALL THESE CONSTANTS ARE READ-ONLY.
    #

    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    # CRAWLER IDENTITY
    #
    _default_user_agent = "simple-web-crawler/v1.0"
    _default_http_headers = {
        'User-Agent': _default_user_agent,
    }

    # CRAWLING LIMITS
    #
    # Each thread can only work with this many urls at a time. This avoids
    # having a thread that takes to long to complete its task.
    _max_urls_per_job = 25
    # Max URLs that can be cached for any host, at any given time. This goes to
    # ensure that we can parallelize the crawling well.
    _max_urls_per_host = 1000
    # Maximum number of hosts that can be cached.
    _max_num_hosts = 10000
    # If this number of failures in a single 'batch' a thread takes to process,
    # the host is considered to be unreachable for the rest of the program.
    _max_num_failures_unreachable = 5
    # Default timeout of a request, in seconds
    _default_timeout = 2
    # Default crawl delay. Is used when fetching of robots.txt fails.
    _default_crawl_delay = 0.1
    # Maximum delay to wait between successive requests to a web page, in
    # seconds.
    _max_crawl_delay = 2
    # Final size of each WARC file.
    _warc_file_page_limit = 1000
    # We let threads work with smaller files, to avoid ruining the parallelism.
    _warc_file_page_shard = _max_urls_per_job * 2

    def __init__(self, config):
        self._config = config

        # Maximum number of threads the pool is allowed to spawn
        self._max_workers = min(
            (
                self._config.page_limit //
                self._warc_file_page_shard
            ),
            128
        )
        # Minimum 5
        self._max_workers = max(self._max_workers, 5)
        # Maximum number of registered jobs, i.e. tasks in the pool at any given
        # time.
        self._max_njobs = self._max_workers * 4

        # _crawl_package is the main global source of information of which pages
        # have already been crawled, and which pages have yet to be crawled.
        self._crawl_package = CrawlPackage()

        # _robots_cache is a map host -> parsed robots.txt file.
        self._robots_cache = {}

        # _default_robots_policy is used in place of a real robots.txt policy in
        # case that one is not reachable.
        self._default_robots_policy = FakeRobotsPolicy(self._max_crawl_delay)

        # _active_hosts is a set used to indicate if a host is actively being
        # crawled by some thread.
        self._active_hosts = set()

        # _unreachable_hosts is a set of hosts for which
        # `self._max_num_failures_unreachable` consecutive requests failed. We
        # don't want to keep retrying, since that will slow us down.
        self._unreachable_hosts = set()

        # _warc_files is a map <file index> -> <number of pages>. This is used
        # by the threads to keep track of how many pages there are in a file.
        self._warc_files = {}
        # _warc_files_lock should be used to atomically access or modify these
        # data structures.
        self._warc_files_lock = Lock()
        # _free_warc_files is a list of file indexes that a thread can pick up
        # to export page information.
        self._free_warc_files = []
        # _cur_warc_file_idx is the highest index of all
        self._cur_warc_file_idx = 0
        # _get_warc_file_name returns an output WARC-format file name, given an
        # index.
        self._get_warc_file_name = lambda idx : (
            str(idx) + "_" + self._config.output_pages_path)

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

        # Clean output WARC files
        for fpath in glob.glob("*_" + self._config.output_pages_path):
            os.remove(fpath)

    # run_timed calls run() and measures elapsed time.
    def run_timed(self):
        before = datetime.now()

        self.run()

        elapsed = datetime.now() - before
        logger.info(f"Elapsed time: {elapsed}")

        return (f"{self._crawl_package.total_crawled} pages were crawled. "+
                f"Elapsed time: {elapsed}")

    # run assumes that Crawler.init has already been called upon the object.
    def run(self):
        with fragile_with(concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers,
        )) as executor:
            results = []

            # Start with seeds
            tocrawl = self._get_tocrawl(self._seeds)
            self._register_urls(self._crawl_package, {}, tocrawl)
            self._submit_jobs(self._crawl_package, executor, results)

            # Go deeper
            iteration = 0
            while True:
                logger.info(
                    f"Starting iteration number {iteration}. "+
                    f"Number of results available: {len(results)}. "+
                    f"Number of crawled pages so far: "+
                    f"{self._crawl_package.total_crawled}. Number of pages "+
                    f"available for crawling: {self._crawl_package.total_tocrawl}. "+
                    f"Total number of hosts: {len(self._crawl_package.tocrawl)}. "+
                    f"Number of active hosts: {len(self._active_hosts)}."
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
                    timeout=2,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                results = list(not_completed)

                for future in completed:
                    self._process_complete_future(self._crawl_package, future)

                self._submit_jobs(self._crawl_package, executor, results)

                iteration += 1

            self._shutdown_threads(executor, results)
            self._aggregate_warc_files()
            self._print_statistics()
            self._force_finish()

    def _submit_jobs(self, crawl_package, executor, results):
        if crawl_package.total_tocrawl == 0:
            return

        # This is the object that we will be filling in the loop below
        tocrawl = {}

        hosts = list(crawl_package.tocrawl.keys())
        while True:
            # Very important to have this maximum jobs limit.
            if len(results) + len(tocrawl) >= self._max_njobs:
                break
            if len(hosts) == 0:
                break

            # Pick a host randomly
            hostidx = random.randint(0, len(hosts)-1)
            host = hosts.pop(hostidx)

            # This condition is what ensures that we will not act over the same
            # host in two different threads!!!
            if host in self._active_hosts:
                # There was a previous bug where this was 'break' instead of
                # 'continue', and so EVERYTHING got messed up...
                continue

            # Add to list of urls to crawl for that host.
            new_urls = []
            for _ in range(self._max_urls_per_job):
                if (crawl_package.tocrawl[host] == None or
                    len(crawl_package.tocrawl[host]) == 0
                ):
                    # logger.info(f"Breaking D. Host: {host}. Length new_urls: {len(new_urls)}. crawl_package.tocrawl[host] = {crawl_package.tocrawl[host]}")
                    crawl_package.remove_tocrawl(host)
                    break
                new_url = crawl_package.pop_tocrawl(host)
                new_urls.append(new_url)
            if len(new_urls) > 0:
                tocrawl[host] = new_urls

        for host in tocrawl:
            if len(results) > max(self._max_workers, 2 * (
                    (self._config.page_limit - crawl_package.total_crawled) //
                    self._warc_file_page_shard
            )):
                return
            results.append(executor.submit(self._crawl, host, tocrawl[host]))
            self._active_hosts.add(host)

    # _process_complete_future takes a completed crawling task and processes
    # it. It removes the crawled host from the list of active hosts, and
    # registers the URLs crawled in the task.
    def _process_complete_future(self, crawl_package, future):
        # .result() must be built to not throw exceptions, otherwise the program
        # might crash here.
        host, num_failures, crawled_urls, new_urls = future.result()

        if num_failures >= self._max_num_failures_unreachable:
            self._crawl_package.remove_tocrawl(host)
            self._unreachable_hosts.add(host)
        self._active_hosts.remove(host)

        tocrawl = self._get_tocrawl(new_urls)

        logger.info(f"Found {len(new_urls)} potentially new URLs")
        self._register_urls(crawl_package, {host: crawled_urls}, tocrawl)

    # _get_tocrawl receives a list of URLs. It returns a dict host->URL
    # containing all URLs given in that list.
    def _get_tocrawl(self, urls):
        tocrawl = {}
        for url in urls:
            _, host, _ = parse_url(url)
            if tocrawl.get(host) == None:
                tocrawl[host] = []
            tocrawl[host].append(url)
        return tocrawl

    # _register_urls puts crawled and to-crawl URLs in the global cache.
    #
    # _register_urls only adds an URL to the global cache of URLs 'to-crawl' if
    # it is a new URL.
    def _register_urls(self, crawl_package, recently_crawled,
                       potentially_new_tocrawl):
        for host in recently_crawled:
            crawl_package.add_urls_crawled(host, recently_crawled[host])

        for host in potentially_new_tocrawl:
            if (host not in crawl_package.tocrawl and
                len(crawl_package.tocrawl) >= self._max_num_hosts
            ):
                continue
            elif (crawl_package.tocrawl.get(host) != None and
                len(crawl_package.tocrawl[host]) >= self._max_urls_per_host
            ):
                continue

            robots_policy = self._robots_cache.get(host)
            if robots_policy == None:
                # Just put the hash entry, but don't fill in the robots
                # policy. Let the worker threads do it, to avoid getting stuck
                # here.
                self._robots_cache[host] = None

            # Check if site can be reached at all. If not, don't bother with
            # it. This can save many seconds of our life.
            if self._is_host_unreachable(host):
                continue

            potentially_new_urls = potentially_new_tocrawl[host]
            new_urls = []
            for url in potentially_new_urls:
                if url in new_urls:
                    continue
                elif not self._is_url_new(crawl_package.crawled, host, url):
                    continue
                elif robots_policy != None and not robots_policy.allowed(url):
                    # Avoid overhead by not including disallowed URLs here.
                    continue
                new_urls.append(url)
            if len(new_urls) > 0:
                crawl_package.add_urls_tocrawl(host, new_urls,
                                               self._max_urls_per_host)

    def _shutdown_threads(self, executor, results):
        logger.info("Shutting down...")
        for i in range(len(results)):
            logger.info(f"Cancelling result number {i}.")
            future = results[i]
            future.cancel()
        # Shutdown MUST come after you have canceled the results!
        executor.shutdown(wait=False)
        logger.info("Shut down.")

    def _force_finish(self):
        raise fragile_with.Break("Terminating program forcedly")

    # _aggregate_warc_files transforms the WARC files that each thread uses into
    # files with the proper size.
    #
    # For example, if each thread is allowed to lock a file that fits 50 pages,
    # and the final chunk size is 1000 pages per file, then this function will
    # do the following: for every 20 files of size 50, it will convert them into
    # a single one with size 1000.
    def _aggregate_warc_files(self):
        logger.info(f"Aggregating WARC files into chunks of size "+
                    f"{self._warc_file_page_limit}.")

        temp_fpath = "web-crawler-temp.gz"
        open(temp_fpath, 'w')

        pages_in_output = 0
        output_fpath_idx = 0
        for fileidx in self._warc_files:
            logger.info(f"Aggregating file with index {fileidx}.")
            input_fpath = self._get_warc_file_name(fileidx)

            # Copy to temp
            with gzip.open(input_fpath, 'rb') as fin:
                with gzip.open(temp_fpath, 'ab') as fout:
                    shutil.copyfileobj(fin, fout)

            if (pages_in_output >= self._warc_file_page_limit or
                fileidx == len(self._warc_files) - 1
            ):
                output_fpath = (f"aggregated{output_fpath_idx}_" +
                                self._config.output_pages_path)
                open(output_fpath, 'w')
                # Copy from temp to output, fixing any compression errors.
                rc = Recompressor(temp_fpath, output_fpath, verbose=False)
                with suppress_output():
                    rc.recompress()
                pages_in_output = 0
                output_fpath_idx += 1
                # Empty temp file
                open(temp_fpath, 'w')

            pages_in_output += self._warc_file_page_shard

        # Cleanup
        logger.info("Cleaning up temporary files.")
        os.remove(temp_fpath)
        for fileidx in self._warc_files:
            fpath = self._get_warc_file_name(fileidx)
            os.remove(fpath)

        logger.info("Done aggregating WARC files.")

    def _print_statistics(self):
        logger.info(f"Number of crawled pages: {self._crawl_package.total_crawled}")
        logger.info(f"Number of unique domains: {len(self._crawl_package.crawled)}")

    def _is_host_unreachable(self, host):
        if host in self._unreachable_hosts:
            return True
        return False

    def _is_relevant_text(self, soup_element):
        if soup_element.parent.name in Crawler._nontext_tags:
            return False
        if isinstance(soup_element, bs4_comment):
            return False
        if str(soup_element) == ' ' or str(soup_element) == '\n':
            return False
        return True

    # _find_relevant_text returns a string with the first 20 relevant words of
    # text in the given BeautifulSoup object.
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

    # _pring_debug prints useful information about a page when the -d flag is
    # activated.
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

    def _use_default_robot_policy(self, host):
        self._robots_cache[host] = self._default_robots_policy

    def _register_robot_policy(self, host, tid="Unknown"):
        if self._robots_cache.get(host) != None:
            return True

        # Try to get robots.txt policy using both HTTP and HTTPS.
        logger.info(f"({tid}) Registering new robots policy for host {host}")
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
                # Previously, we used to return False here, and not set any
                # policy. But that led to an infinite loop of retrying for some
                # host.
                self._use_default_robot_policy(host)
                return True
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

        # Cache the info
        policy = robots_resp.agent(Crawler._default_user_agent)
        self._robots_cache[host] = policy

        return True

    def _get_crawl_delay_from_policy(self, robots_policy):
        crawl_delay = robots_policy.delay
        if crawl_delay == None:
            crawl_delay = self._default_crawl_delay
        elif crawl_delay > self._max_crawl_delay:
            crawl_delay = self._max_crawl_delay
        return crawl_delay

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
        elif (href.startswith('javascript:') or
              href.startswith('mailto:') or
              href.startswith('tel:')
        ):
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

    def _pop_warc_file(self):
        self._warc_files_lock.acquire()

        if len(self._free_warc_files) > 0:
            fileidx = self._free_warc_files.pop()
            warc_file = self._get_warc_file_name(fileidx)
            already_crawled = self._warc_files[fileidx]
        elif (len(self._warc_files) * self._warc_file_page_shard >=
             self._config.page_limit
        ):
            self._warc_files_lock.release()
            return None, None, None, False
        else:
            # Create new entry. The file itself is not created.
            fileidx = self._cur_warc_file_idx
            warc_file = self._get_warc_file_name(fileidx)
            already_crawled = 0
            # Update global entries. Notice that the file starts as 'not free',
            # so we don't include it in the free files list.
            self._warc_files[fileidx] = 0
            self._cur_warc_file_idx += 1

        logger.debug(f"Locked file '{warc_file}'. Number of free files: "+
                     f"{len(self._free_warc_files)}")
        self._warc_files_lock.release()
        return fileidx, warc_file, already_crawled, True

    def _release_warc_file(self, fileidx, already_crawled):
        self._warc_files_lock.acquire()

        if already_crawled < self._warc_file_page_shard:
            # Only add back to free list if not full already.
            self._free_warc_files.append(fileidx)
        self._warc_files[fileidx] = already_crawled

        logger.debug(f"Released file '{fileidx}'. Number of free files: "+
                     f"{len(self._free_warc_files)}")

        self._warc_files_lock.release()
        return

    def _filter_bad_requests(self, req, resp, req_recorder):
        if resp.http_headers.get_statuscode() != '200':
            return None, None
        return req, resp

    def _safe_request(self, http_pool, request_type, url, tid="Unknown"):
        try:
            resp = http_pool.request(request_type, url,
                                     timeout=self._default_timeout)
        except urllib3.exceptions.MaxRetryError as e:
            logger.info(f"({tid}) Timeout for url '{url}': {e}")
            return None
        except Exception as e:
            logger.info(f"({tid}) Unknown error when requesting for '{url}': {e}")
            return None
        return resp

    def _filter_html_pages(self, http_pool, crawl_delay, urls, tid="Unknown"):
        html_pages = []
        num_failures = 0
        for url in urls:
            resp = self._safe_request(http_pool, 'HEAD', url, tid=tid)
            if resp == None:
                num_failures += 1
                continue

            content_types = resp.getheaders().get(CONTENT_TYPE_KEY)
            if (content_types == None or
                VALID_CONTENT_TYPE not in content_types
            ):
                continue

            html_pages.append(url)

            time.sleep(crawl_delay)

        return html_pages, num_failures

    def _find_hrefs_in_soup(self, soup):
        links = soup.findAll('a')
        hrefs = [link.attrs.get('href') for link in links]
        return hrefs

    def _crawl_url(self, http_pool, url, tid="Unknown"):
        logger.debug(f"({tid}) Crawling url: {url}")
        resp = self._safe_request(http_pool, 'GET', url, tid=tid)
        if resp == None:
            return None

        logger.debug(f'({tid}) Got response status: {resp.status}')

        try:
            soup = BeautifulSoup(resp.data, SOUP_PARSER)
            
            if self._config.debug:
                self._print_debug(url, soup)
                
            # Transform hrefs into normalized URLs based on parent
            hrefs = self._find_hrefs_in_soup(soup)
            normalized_child_urls = self._get_href_normalized_urls(url, hrefs)
        except Exception as e:
            logger.info(f"({tid}) Error parsing '{url}': {e}")
            return None

        return normalized_child_urls

    def _crawl(self, host, parent_urls):
        tid = get_ident()
        logger.debug(f"({tid}) Started crawling. Received "+
                     f"host: '{host}', parent_urls: {parent_urls}.")

        crawled_parent_urls = []
        crawled_child_urls = []
        num_failures = 0

        try:
            # Get robots.txt information to avoid being blocked.
            if self._register_robot_policy(host, tid=tid) == False:
                log.info(f"({tid}) Something went wrong when registering robot "+
                         f"policy. Returning with no-op.")
                # Make sure that these URLs return to the 'to crawl' list.
                crawled_child_urls.extend(parent_urls)
                return host, 0, [], crawled_child_urls
            robots_policy = self._robots_cache[host]
            crawl_delay = self._get_crawl_delay_from_policy(robots_policy)

            # Check if page contains HTML content beforehand. Getting only the
            # header avoids overheads if the target page has a lot of content.
            #
            # This needs a separate pool manager, because WARCIO's capture_http
            # needs to come before the connection on the pool is stabilished.
            head_http_pool = urllib3.PoolManager(headers=Crawler._default_http_headers)
            html_urls, num_failures = self._filter_html_pages(head_http_pool,
                                                              crawl_delay,
                                                              parent_urls,
                                                              tid=tid)
            if num_failures >= self._max_num_failures_unreachable:
                logger.info(f"({tid}) Reached max number of failures "+
                            f"for host '{host}'.")
                return host, num_failures, [], []

            # This call should properly apply mutual exclusion to avoid data
            # races. We are also obliged to call the 'release' counterpart in
            # the same thread before returning.
            fileidx, warc_file, already_crawled, success = self._pop_warc_file()
            if success != True:
                # This may indicate that there are no free files, and the limit
                # number of files has already been reached, for example.
                logger.info(f"({tid}) Unable to lock a WARC file. "+
                            f"Returning with no-op.")
                # Make sure that these URLs return to the 'to crawl' list.
                crawled_child_urls.extend(parent_urls)
                return host, 0, [], crawled_child_urls
            logger.debug(f"({tid}) Capturing pages to '{warc_file}'")

            http_pool = urllib3.PoolManager(headers=Crawler._default_http_headers)
            with capture_http(warc_file, self._filter_bad_requests):
                for parent_url in html_urls:
                    if (already_crawled + len(crawled_parent_urls) >=
                        self._warc_file_page_shard
                    ):
                        logger.info(f"({tid}) Reached max number of pages for "+
                                    "WARC file.")
                        break

                    if not robots_policy.allowed(parent_url):
                        continue

                    # Here is the main call, where the GET request will be made.
                    child_urls = self._crawl_url(http_pool, parent_url, tid=tid)

                    if child_urls == None:
                        num_failures += 1
                        if num_failures >= self._max_num_failures_unreachable:
                            logger.info(f"({tid}) Reached max number of failures "+
                                        f"for host '{host}'.")
                            break
                        continue

                    crawled_parent_urls.append(parent_url)
                    crawled_child_urls.extend(child_urls)

                    logger.debug(f"({tid}) Sleeping crawl delay of "+
                                 f"{crawl_delay} seconds")
                    time.sleep(crawl_delay)

            self._release_warc_file(fileidx,
                                    already_crawled + len(crawled_parent_urls))
        except Exception as e:
            logger.error(f"({tid}) Received uncaught exception while crawling: "+
                         f"{e}. Returning immediately.", exc_info=True)
            try:
                self._release_warc_file(fileidx,
                                        already_crawled + len(crawled_parent_urls))
            except:
                logger.error(f"({tid}) Error releasing WARC file: "+
                             f"{e}. Returning immediately.", exc_info=True)
                pass

        logger.debug(f"({tid}) Finished crawling.")

        return host, num_failures, crawled_parent_urls, crawled_child_urls
