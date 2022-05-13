from contextlib import contextmanager
import os
import sys

# Read-only global variables
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
HTTP_SCHEME = "http"
HTTPS_SCHEME = "https"
VALID_PROTOCOLS = [HTTP_SCHEME, HTTPS_SCHEME]
DEFAULT_PROTOCOL = HTTPS_SCHEME
VALID_CONTENT_TYPE = "text/html"
SOUP_PARSER = "html.parser"
CONTENT_TYPE_KEY = "Content-Type"
ROBOTS_TXT_SUFFIX = "/robots.txt"
MAX_URL_LENGTH = 2048

def file_exists(fpath):
    return os.path.exists(fpath)

def crawler_run_result(status, reason=""):
    result = {}
    result['Status'] = status
    if reason != '':
        result['Reason'] = reason
    return result

def between(n, left, right):
    return left <= n and n <= right

def parse_url(url):
    split_by_protocol = url.split("://")
    if len(split_by_protocol) == 1:
        protocol = DEFAULT_PROTOCOL
        url_body = split_by_protocol[0].split("?")[0]
    else:
        protocol = split_by_protocol[0]
        url_body = split_by_protocol[1].split("?")[0]
    body_split_by_slash = url_body.split("/")
    host = body_split_by_slash[0]
    # Remove everything after 
    path_list = body_split_by_slash[1:]

    # Remove final '/' if exists, for consistency
    if len(path_list) > 0:
        if path_list[-1] == '':
            path_list.pop()
        if len(path_list) > 0:
            path_list = [''] + path_list

    return protocol, host, path_list

def is_valid_url(url):
    if len(url) > MAX_URL_LENGTH:
        return False

    split_by_protocol = url.split("://")
    if (len(split_by_protocol) >= 2 and
        split_by_protocol[0] not in VALID_PROTOCOLS
    ):
            return False
    elif len(split_by_protocol) == 1:
        split_by_slash = split_by_protocol[0].split('/')
    else:
        split_by_slash = split_by_protocol[1].split('/')
    url_body = split_by_slash[0]
    domains = url_body.split('.')
    if len(domains) < 2:
        return False
    # If 'domain' is actually .html for example, we don't want to say this is a
    # valid domain.
    if domains[-1] in ['xml', 'html', 'pdf', 'php']:
        return False

    return True

def cache_urls(cache, host, urls):
    if cache.get(host) == None:
        cache[host] = []
    cache[host].extend(urls)

def len_two_dicts_entry(d1, d2, k):
    v1 = d1.get(k)
    v2 = d2.get(k)
    if v1 != None:
        if v2 != None:
            return len(v1) + len(v2)
        else:
            return len(v1)
    elif v2 != None:
        return len(v2)
    else:
        return 0

def get_robots_url(scheme, host):
    return scheme + "://" + host + ROBOTS_TXT_SUFFIX

@contextmanager
def suppress_output():
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

# # This class' implementation was obtained from stack overflow:
# #
# # https://stackoverflow.com/questions/11195140/break-or-exit-out-of-with-statement
# class fragile_with(object):
#     class Break(Exception):
#       """Break out of the with statement"""

#     def __init__(self, value):
#         self.value = value

#     def __enter__(self):
#         return self.value.__enter__()

#     def __exit__(self, etype, value, traceback):
#         error = self.value.__exit__(etype, value, traceback)
#         if etype == self.Break:
#             return True
#         return error
