##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os
import logging
import typing
from logging import Logger
from datetime import datetime

class LogBase:
    """
    Base class to derive from for classes that wish to have logging capabilities to
    a mounted file share.
    """
    def __init__(self, log_name:str, file_share:str, identity:str = None, is_workflow:bool = False):
        """
        Constructor

        log_name:
            Name to put in the log, typically related to the class deriving
        file_share:
            Path of the file share to put files into. Use "./" for local storage.
        identity:
            If provided, is appended to the end of the log name. Used with GUID's to 
            uniquely identify processes. 
        is_workflow:
            Flag used to determine file name (load_ or workload_)
        """
        self.file_share = file_share
        self.log_name = log_name
        self.identity = identity
        self.is_workflow = is_workflow

    def get_logger(self) -> Logger:
        """
        Gets a logger instance for any class function that derives from this class.
        """
        return LoggingUtils.get_logger(self.file_share, self.log_name, self.identity, self.is_workflow)


class LoggingUtils:
    """
    Generic logging util.
    """

    LOG_UTILS:typing.Dict[str, Logger] = {}
    ACTIVE_LOG_FILE = None
    LOG_BASE = "output"

    @staticmethod
    def get_logger(file_share:str, log_name:str, identity:str = None,  is_workflow:bool = False) -> Logger:
        """
        Constructor

        log_name:
            Name to put in the log, typically related to the class deriving
        file_share:
            Path of the file share to put files into. Use "./" for local storage.
        identity:
            If provided, is appended to the end of the log name. Used with GUID's to 
            uniquely identify processes. 
        is_workflow:
            Flag used to determine file name (load_ or workload_)
        """

        if log_name in LoggingUtils.LOG_UTILS:
            return LoggingUtils.LOG_UTILS[log_name]

        log_base_start = "stgscan"
        if is_workflow:
            log_base_start = "workload"

        # Set up base logger
        timestamp = datetime.now().strftime('%m%d%y')
        LOG_FILE_NAME = "{}-{}.log".format(log_base_start, timestamp)
        if identity is not None:
            LOG_FILE_NAME = "{}-{}.log".format(log_base_start, identity)

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