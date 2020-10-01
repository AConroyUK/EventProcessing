import logging
from logging import handlers

class logHandler:
    def __init__(self):
        with open("eventprocessing.log", 'w') as logfile:
            pass
        self.logger = logging.getLogger("eventprocessing.log")
        formatter = logging.Formatter(
            '%(asctime)s | %(threadName)-15s |  %(levelname)s: %(message)s')
        self.logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        logFilePath = "eventprocessing.log"
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=logFilePath, when='midnight', backupCount=30)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def info(self,string):
        try:
            self.logger.info(string)
        except:
            self.logger.info("Failed to process logging string")

    def error(self,string):
        try:
            self.logger.error(string)
        except:
            self.logger.error("Failed to process logging string")

    def debug(self,string):
        try:
            self.logger.debug(string)
        except:
            self.logger.error("Failed to process logging string")
