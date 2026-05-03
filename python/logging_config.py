

import logging
from paths import PYTHON_DIR

def setup_logging():
    logging.basicConfig(
        filename=PYTHON_DIR/'pipeline.log',
        level=logging.ERROR,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )







