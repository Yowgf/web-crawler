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
