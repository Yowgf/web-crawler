import json
import time

from bs4 import BeautifulSoup
import requests

from . import log

logger = log.logger()

class Crawler:
    def __init__(self, config):
        self._config = config

    def init(self):
        pass

    def print_debug(self, resp):
        debug_json_obj = {}

        soup = BeautifulSoup(resp.text, 'html.parser')

        debug_json_obj['URL'] = resp.url
        debug_json_obj['Title'] = str(soup.title.string)

        text = ""
        for child in soup.descendants:
            if child.name == None:
                text = child
                break

        debug_json_obj['Text'] = text
        debug_json_obj['Timestamp'] = int(time.time())

        print(json.dumps(debug_json_obj))

    def run(self):
        url = 'https://g1.globo.com/'

        logger.debug('Making request to url ' + url)

        resp = requests.get(url)

        logger.debug('Got respose: ' + str(resp))
#        logger.debug('Got html content: ' + resp.text)

        if self._config.debug:
            self.print_debug(resp)
