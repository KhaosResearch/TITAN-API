import logging.config


def configure_logging():
    DEFAULT_LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "basic": {
                "format": "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {
                "formatter": "basic",
                "class": "logging.StreamHandler",
            },
            "rotate_file": {
                "formatter": "basic",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "titan.log",
                "encoding": "utf8",
                "maxBytes": 100000,
                "backupCount": 1,
            },
        },
        "loggers": {
            "": {
                "level": "INFO",
                "handlers": ["rotate_file"],
            },
            "titan": {
                "level": "DEBUG",
                "handlers": ["console"],
            },
        },
    }

    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)


def get_logger(module):
    return logging.getLogger(module)
