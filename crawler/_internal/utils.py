import os

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"

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

def get_host(url, url_depth=1):
    host = None
    split_by_protocol = url.split("://")
    if len(split_by_protocol) > 1:
        host = "/".join(split_by_protocol[1].split("/")[:url_depth])
    else:
        host = "/".join(split_by_protocol[0].split("/")[:url_depth])
    return host
