import os
import sys
import logging
from datetime import datetime
from configparser import ConfigParser

# Read Config file
cfg_file = 'config.ini'
config = ConfigParser()
config.read(cfg_file, encoding="utf-8")

# Storing the contents of the config file into respective dictionary variables
db_params = dict(config.items('db_params'))


# Logging Configuration
LOG_FILE=f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
logs_path=os.path.join(os.getcwd(),"logs",LOG_FILE)
os.makedirs(logs_path, exist_ok=True)  # if the folder exists, keep on appending the log file to the folder itself

LOG_FILE_PATH=os.path.join(logs_path,LOG_FILE)


logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)


# Custom Exception Configuration
def error_message_detail(error, error_detail:sys):
    _,_,exc_tb=error_detail.exc_info()    # Throws which file and which line an exception is occurring in
    file_name=exc_tb.tb_frame.f_code.co_filename    # Gets the file name
    error_message="Error occurred in python script name [{0}] line number [{1}] error message [{2}]".format(
        file_name,exc_tb.tb_lineno,str(error)
    )
    return error_message


class CustomException(Exception):
    def __init__(self, error_message, error_detail:sys):
        super().__init__(error_message)
        self.error_message=error_message_detail(error_message, error_detail=error_detail)

    def __str__(self):
        return self.error_message