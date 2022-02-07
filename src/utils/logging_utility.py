'''Logging utility docstrings

This module provides various custom Logging classes for logging. The classes
are conveniently named and are self explanantory.

This module internally uses the python standard `logging` module. Thus debug,
info, exception etc are called internally as would be if using Logging module
directly.

List of Custom Log Classes:


'''

import os
from pathlib import Path
import logging
from logging import NOTSET, StreamHandler
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import sys

from src.config.definitions import MB


LOG_DIR = Path('logs').resolve()
# 2022-01-30 23:21:18- ERROR - ...\src\process_source_system\example.py - class_name >> caller_func - TypeError occured.
FORMAT='%(asctime)2s - %(levelname)2s - %(name)s - %(message)s'
DATEFMT='%y-%m-%d-%H:%M:%S'
ROOT_LOG_FILE = "app.log"
INFO_LOG_FILE = "info.log"
DEBUG_LOG_FILE = "debug.log"
EXCEPTION_ERROR_LOG_FILE = "error.log"
EXCEPTION_INFO_LOG_FILE = "error_info.log"

DEBUG_NAME = "DebugLogger"
INFO_NAME = "InfoLogger"
EXCEPTION_ERROR_NAME = "ExceptionErrorLogger"
EXCEPTION_INFO_NAME = "ExceptionInfoLogger"

LOG_ROLLOVER_SIZE = 5 * MB
NUM_ROLLOVERS_ALLOWED = 5


class RootLogger:

    def __init__(self, format=FORMAT, date_format = DATEFMT) -> None:

        self.format = format
        self.date_format = date_format
        
        # Logger Level - Set to NOTSET if you have child loggers with pre-defined levels
        logging.basicConfig(level=NOTSET 
            , format=self.format
            , datefmt=self.date_format
            , handlers=[RotatingFileHandler(os.path.join(LOG_DIR, ROOT_LOG_FILE), mode='a', maxBytes=10*MB)]
        )


class DebugLogger(RootLogger):

    def __init__(self, format=FORMAT, date_format=DATEFMT):
        
        super().__init__(format, date_format)
        # set name of the module
        self.logger_name = "DebugLogger"#caller_module_path      

    def log(self, caller_path, message):
        '''
        This will log at DEBUG level.
        '''
        log_msg = str(caller_path) + '  -  '+ message

        logger = logging.getLogger(self.logger_name)
    
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        
        # define formatters - defined in super class
        formatter = logging.Formatter(self.format, datefmt=self.date_format)

        ### define Handlers ###
        
        timeRotating_handler = TimedRotatingFileHandler(os.path.join(LOG_DIR, DEBUG_LOG_FILE)
                                    , when='midnight', utc=True, backupCount=10)
        timeRotating_handler.setLevel(logging.DEBUG)
        timeRotating_handler.setFormatter(formatter)

        logger.addHandler(timeRotating_handler)

        logger.debug(log_msg)


class InfoLogger(RootLogger):
    
    def __init__(self, format=FORMAT, date_format=DATEFMT
            , rotation_size=LOG_ROLLOVER_SIZE, num_rollovers_allowed = NUM_ROLLOVERS_ALLOWED
        ):
        
        super().__init__(format, date_format)
        
        self.logger_name = INFO_NAME
        self.rotation_size = rotation_size
        self.rotation_limit = num_rollovers_allowed
    
    def log(self, caller_path, message):
        '''
        path: Fully qualified path name of the calling code block including
              Class name and function name if available.

        message: Any desired message as required
        '''
        log_msg = str(caller_path) + '  -  '+ message
        
        logger = logging.getLogger(self.logger_name)

        logger.setLevel(logging.INFO)
        logger.propagate = True

        formatter = logging.Formatter(self.format, datefmt=self.date_format)

        # define Handlers
        # backupCount --> number of rollovers allowed, not number of actual backups of current log file
        rotating_Handler = RotatingFileHandler(os.path.join(LOG_DIR, INFO_LOG_FILE)
                            , maxBytes=self.rotation_size, backupCount=self.rotation_limit
                        )
        rotating_Handler.setLevel(logging.INFO)
        rotating_Handler.setFormatter(formatter)

        logger.addHandler(rotating_Handler)

        logger.info(log_msg)
        

class ExceptionErrorLogger(RootLogger):
    # with Stack Trace, but Exec Info is Un-necessary
    def __init__(self, format=FORMAT, date_format=DATEFMT
            , rotation_size= 10*MB, num_rollovers_allowed = NUM_ROLLOVERS_ALLOWED
        ):
        
        super().__init__(format, date_format)
        
        self.logger_name = EXCEPTION_ERROR_NAME
        self.rotation_size = rotation_size
        self.rotation_limit = num_rollovers_allowed
    
    def log(self, caller_path, message):
        '''
        path: Fully qualified path name of the calling code block including
              Class name and function name if available.

        message: Any desired message as required
        '''
        log_msg = str(caller_path) + '  -  '+ message
        
        logger = logging.getLogger(self.logger_name)

        logger.setLevel(logging.ERROR)
        logger.propagate = False

        formatter = logging.Formatter(self.format, datefmt=self.date_format)

        ### define Handlers ###

        # backupCount --> number of rollovers allowed, not number of actual backups of current log file
        rotating_Handler = RotatingFileHandler(os.path.join(LOG_DIR, EXCEPTION_ERROR_LOG_FILE)
                            , maxBytes=self.rotation_size, backupCount=self.rotation_limit
                        )
        rotating_Handler.setLevel(logging.ERROR)
        rotating_Handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(stream=sys.stdout) # log to console
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        
        logger.addHandler(rotating_Handler)
        logger.addHandler(console_handler)        
        
        # Actual Log Statement
        logger.exception(log_msg, stack_info=True, exc_info=False)
    

class ExceptionInfoLogger(RootLogger):
    # with Exec Info, but No Trace back
    def __init__(self, format=FORMAT, date_format=DATEFMT
            , rotation_size= 10*MB, num_rollovers_allowed = NUM_ROLLOVERS_ALLOWED
        ):
        
        super().__init__(format, date_format)
        
        self.logger_name = EXCEPTION_INFO_NAME
        self.rotation_size = rotation_size
        self.rotation_limit = num_rollovers_allowed
    
    def log(self, caller_path, message):
        '''
        path: Fully qualified path name of the calling code block including
              Class name and function name if available.

        message: Any desired message as required
        '''
        
        log_msg = str(caller_path) + '  -  '+ message
        
        logger = logging.getLogger(self.logger_name)

        logger.setLevel(logging.ERROR)
        logger.propagate = True

        formatter = logging.Formatter(self.format, datefmt=self.date_format)

        ### define Handlers ###
    
        timeRotating_handler = TimedRotatingFileHandler(os.path.join(LOG_DIR, EXCEPTION_INFO_LOG_FILE)
                                    , when='midnight', utc=True, backupCount=10)
        timeRotating_handler.setLevel(logging.ERROR)
        timeRotating_handler.setFormatter(formatter)
    
    
        logger.addHandler(timeRotating_handler)

        # Actual Log Statement    
        logger.exception(log_msg, exc_info=False, stack_info=True)
    