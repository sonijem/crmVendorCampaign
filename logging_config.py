import logging

def get_logger():
    """
    To get default logger for the application
    """
    # set logging level
    format = "%(asctim)s: %(filename)s-%(lineno)d: %(message)s"
    logging.basicConfig(encoding='utf-8', format=format, datefmt="%H:%M:%S")

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    return logger

logger = get_logger()