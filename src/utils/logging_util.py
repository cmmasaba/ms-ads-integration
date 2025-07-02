"""Logging module integrated with GCP"""
import os
import logging as pylogging
from google.cloud import logging
from google.cloud.logging.handlers import CloudLoggingHandler

def name() -> str:
    """Get the GCP logger name"""
    return os.environ["GCP_LOGGING_SERVICE_NAME"]

class GclClient:
    """Initialize the GCP logger"""
    def __init__(self) -> None:
        self.gcl_client = logging.Client.from_service_account_json(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"],project=os.environ["PROJECT_NAME"])
        self.handler = CloudLoggingHandler(self.gcl_client, name=name())
        formatter = pylogging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        file_handler = pylogging.FileHandler("debug.log", mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger = pylogging.getLogger(name())
        logger.addHandler(self.handler)
        logger.addHandler(file_handler)
        logger.setLevel(pylogging.INFO)
        self.logger = logger


    def get_logger(self) -> pylogging.Logger:
        """Return the logging client"""
        return self.logger

    def close_logger(self) -> None:
        """Manually close the logging handler to flush pending logs."""
        if hasattr(self, 'handler') and self.handler:
            self.handler.close()
            self.gcl_client.close()
