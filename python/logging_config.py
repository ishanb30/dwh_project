

import logging
from paths import PYTHON_DIR

def setup_logging():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    handler = logging.FileHandler(PYTHON_DIR/'pipeline.log')
    handler.setLevel(logging.ERROR)
    handler.setFormatter(formatter)
    logger.addHandler(handler)






