__author__ = 'dlahr'

import logging
import logging.handlers
import ConfigParser
import os.path


_default_dashboard_logger_config_file = os.path.expanduser("~/.cmap_dashboard_logger.cfg")

LOGGER_NAME = "cmap_logger"
DASHBOARD_LOGGER_NAME = "cmap_dashboard_logger"

_LOG_FORMAT = "%(levelname)s %(asctime)s %(module)s %(funcName)s %(message)s"
_LOG_FILE_MAX_BYTES = 10000000
_LOG_FILE_BACKUP_COUNT = 5

_DASHBOARD_LOG_FORMAT = "%(levelname)s %(asctime)s %(message)s"


def setup(verbose=False, log_file=None):
    logger = logging.getLogger(LOGGER_NAME)

    level = (logging.DEBUG if verbose else logging.INFO)

    if log_file is None:
        logging.basicConfig(level=level, format=_LOG_FORMAT)
    else:
        logger.setLevel(level)
        handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=_LOG_FILE_MAX_BYTES,
                                                       backupCount=_LOG_FILE_BACKUP_COUNT)
        handler.setFormatter(logging.Formatter(fmt=_LOG_FORMAT))
        logger.addHandler(handler)


def setup_dashboard(config_file=_default_dashboard_logger_config_file):
    logger = logging.getLogger(DASHBOARD_LOGGER_NAME)

    cp = ConfigParser.RawConfigParser()
    cp.read(config_file)
    log_filepath = cp.get("standard", "log_filepath")

    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(log_filepath, 
        maxBytes=_LOG_FILE_MAX_BYTES, backupCount=_LOG_FILE_BACKUP_COUNT)
    handler.setFormatter(logging.Formatter(fmt=_DASHBOARD_LOG_FORMAT))
    logger.addHandler(handler)

