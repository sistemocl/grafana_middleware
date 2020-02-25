# -*- coding: utf-8 -*-
import logging
from logging.handlers import RotatingFileHandler

LOG_USE_STREAM = True

def get_logger(name="gtp_migration"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # Check if handlers are already added
    if len(logger.handlers) > 0:
        return logger
    # File and stream handler
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
    fileHandler = RotatingFileHandler("log/{}.log".format(name), mode='a', maxBytes=5 * 1024 * 1024,
        backupCount=1, encoding=None, delay=0)
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    if LOG_USE_STREAM:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)
    return logger
