import json
import time

from bs4 import BeautifulSoup
from bs4.element import Comment as bs4_comment
import requests

from . import log

logger = log.logger()

class Crawler:
    _nontext_tags = ['head', 'meta', 'script', 'style', 'title', '[document]']
    _text_tags = ['span', 'div', 'b', 'strong', 'i', 'em', 'mark', 'small']

    def __init__(self, config):
        self._config = config

    def init(self):
        pass

    def is_relevant_text(self, soup_element):
        if soup_element.parent.name in Crawler._nontext_tags:
            return False
        if isinstance(soup_element, bs4_comment):
            return False
        if str(soup_element) == ' ' or str(soup_element) == '\n':
            return False
        return True

    # filter_relevant_text returns a list with elements considered to be
    # relevant texts, given a list of raw texts.
    #
    # see is_relevant_text
    def filter_relevant_text(self, soup_elements):
        relevant_texts = []
        for element in soup_elements:
            if self.is_relevant_text(element):
                relevant_texts.append(element)
        return relevant_texts

    # find_relevant_text is like filter_relevant_text, but specialized to return
    # the first relevant text, instead of a list
    def find_relevant_text(self, soup_elements):
        for element in soup_elements:
            if self.is_relevant_text(element):
                return element

    def print_debug(self, resp):
        debug_json_obj = {}

        soup = BeautifulSoup(resp.text, 'html.parser')

        debug_json_obj['URL'] = resp.url
        debug_json_obj['Title'] = str(soup.title.string)

        texts = soup.findAll(text=True)
        first_relevant_text = self.find_relevant_text(texts)
        first_20words_relevant_text = str(first_relevant_text).split(" ")[:20]

        debug_json_obj['Text'] = " ".join(first_20words_relevant_text)
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
