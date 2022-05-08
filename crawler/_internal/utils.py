import os

# Read-only global variables
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
VALID_PROTOCOLS = ["http", "https"]
DEFAULT_PROTOCOL = "http"
VALID_CONTENT_TYPE = "html"
SOUP_PARSER = "html.parser"
CONTENT_TYPE_KEY = "Content-Type"

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
        protocol = ""
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
    if len(url) > 2048:
        return False

    split_by_protocol = url.split("://")
    if len(split_by_protocol) < 2:
        return False
    else:
        if split_by_protocol[0] not in VALID_PROTOCOLS:
            return False
    split_by_slash = split_by_protocol[1].split('/')
    url_body = split_by_slash[0]
    domains = url_body.split('.')
    if len(domains) < 2:
        return False
    return True

def cache_urls(cache, host, urls):
    if cache.get(host) == None:
        cache[host] = set(urls)
    else:
        cache[host] = cache[host].union(urls)
