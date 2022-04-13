import os
import shutil
import json
import logging
import typing
from logging import Logger
from datetime import datetime

class LogBase:
    def __init__(self, log_name:str, file_share:str, identity:str = None):
        self.file_share = file_share
        self.log_name = log_name
        self.identity = identity

    def get_logger(self) -> Logger:
        return LoggingUtils.get_logger(self.file_share, self.log_name, self.identity)

class ActivityLog:
    ACTIVITY_BASE = "activity"

    def __init__(self, file_share:str, activity_name:str, identity:str = None):
        # Set up base logger

        self.file_share = file_share
        self.activity_name = activity_name

        if identity is not None:
            if identity is not None:
                self.activity_file = "{}-{}.log".format(self.activity_name,identity)
        else:
            timestamp = datetime.now().strftime('%m%d%y')
            self.activity_file = "{}-{}.log".format(self.activity_name,timestamp)

        self.log_path = os.path.join(self.file_share, ActivityLog.ACTIVITY_BASE)
        self.activity_log_path = os.path.join(self.log_path, self.activity_file)

        self._buffer:typing.List[str] = []

    def add_activity(self, *args):
        now = datetime.utcnow()
        stamp = now.strftime("%m-%d-%Y, %H:%M:%S,%f : ")
        for arg in args:
            content = arg
            if isinstance(arg, dict) or isinstance(arg, list):
                content = json.dumps(arg, indent=4)

            self._buffer.append("{} {}".format(stamp, content))
    
    def dump(self):
        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)
        
        with open(self.activity_log_path, "w") as log:
            for activity in self._buffer:
                log.writelines("{}\n".format(activity))


class LoggingUtils:
    LOG_UTILS:typing.Dict[str, Logger] = {}
    ACTIVE_LOG_FILE = None
    LOG_BASE = "output"

    @staticmethod
    def get_logger(file_share:str, log_name:str, identity:str = None) -> Logger:

        if log_name in LoggingUtils.LOG_UTILS:
            return LoggingUtils.LOG_UTILS[log_name]

        # Set up base logger
        timestamp = datetime.now().strftime('%m%d%y')
        LOG_FILE_NAME = "dataloader-{}.log".format(timestamp)
        if identity is not None:
            LOG_FILE_NAME = "dataloader-{}.log".format(identity)

        working_directory = file_share
        if not working_directory:
            working_directory = os.getcwd()

        LOG_FILE_PATH = os.path.join(
            working_directory, 
            LoggingUtils.LOG_BASE, 
            LOG_FILE_NAME
        )
        LoggingUtils.ACTIVE_LOG_FILE = LOG_FILE_PATH

        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

        handler = logging.FileHandler(LOG_FILE_PATH)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(name)-14.14s] [%(levelname)-7.7s]  %(message)s"))
        
        logger = logging.getLogger(log_name)        

        try:
            level = logging.getLevelName(os.environ.get('LOG_LEVEL', 'info').upper())
            logger.setLevel(level)
        except ValueError:
            print('Valid Log Levels are DEBUG, INFO, WARN and ERROR')
            exit(1)

        logger.addHandler(handler)

        LoggingUtils.LOG_UTILS[log_name] = logger

        return LoggingUtils.LOG_UTILS[log_name]