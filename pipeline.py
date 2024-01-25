# Main libraries import
import logging
import sys

import pandas as pd

logging.basicConfig(filename='development.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

logging.info("Job completed")
logging.debug(sys.argv)

day = sys.argv[1]

print(f"Job completed for day {day}.")