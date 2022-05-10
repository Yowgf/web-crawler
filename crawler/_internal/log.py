import logging

FORMATTER = logging.Formatter('[%(levelname)s] %(asctime)s %(message)s')

all_loggers_map = {}

def stream_handler():
    h = logging.StreamHandler()
    h.setFormatter(FORMATTER)
    return h

def logger():
    global all_loggers_map

    logger_name = 'web-crawler'

    if all_loggers_map.get(logger_name):
        return all_loggers_map.get(logger_name)
    else:
        logger = logging.getLogger(logger_name)
        for h in logger.handlers:
            logger.removeHandler(h)
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        logger.addHandler(stream_handler())

        all_loggers_map[logger_name] = logger
        return logger
