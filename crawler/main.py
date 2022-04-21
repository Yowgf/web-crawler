import sys

from ._internal.config import parse_config
from ._internal.crawler import Crawler
from ._internal.utils import crawler_run_result, STATUS_SUCCESS, STATUS_FAILED
from ._internal import log

logger = log.logger()

def main():
    success_message = ""

    try:
        logger.info("Parsing config.")

        cfg = parse_config(sys.argv[1:])

        logger.info(
            "Successfully parsed config. Config in json format:\n" + cfg.to_json()
        )

        logger.info("Initializing crawler.")

        crawler = Crawler(cfg)
        crawler.init()

        logger.info("Successfully initialized crawler.")

        logger.info("Starting crawler run.")

        success_message = crawler.run()

        logger.info("Crawler ran successfully.")

        return crawler_run_result(status=STATUS_SUCCESS, reason=success_message)

    except Exception as e:
        logger.critical(e, exc_info=True)

        return crawler_run_result(status=STATUS_FAILED, reason=f"Crawler run failed: {e}")
