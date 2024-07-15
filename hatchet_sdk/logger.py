import sys
import logging

# logging config
logging.basicConfig(
    level=logging.ERROR,
    format='[%(levelname)s] hatchet -- %(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()