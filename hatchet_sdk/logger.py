import os
import sys

from loguru import logger

# loguru config
config = {
    "handlers": [
        {"sink": sys.stdout, "format": "[{level}] hatchet -- {time} - {message}"},
    ],
}

logger.configure(**config)
