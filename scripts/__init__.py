import logging

console = logging.StreamHandler()
logger = logging.getLogger("loggger")
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(console)
logger.setLevel(logging.INFO)

DATE_FORMATS = ("%m/%d/%y", "%d.%m.%Y", "%m-%d-%y")
