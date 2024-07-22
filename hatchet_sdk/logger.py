import logging
import sys

# Create a named logger
logger = logging.getLogger("hatchet")
logger.setLevel(logging.ERROR)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] hatchet -- %(asctime)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.propagate = False
