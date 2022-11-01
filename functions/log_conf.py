import yaml
import time
import os
import logging
from logging.handlers import TimedRotatingFileHandler

# We get the current directory 
current_dir = os.getcwd()
all_data_file = current_dir + '\\data.parquet'
log = current_dir + '\\log.txt'

current_dir = os.getcwd()

# We open the configuration file and we load it into the config variable
yaml_file = current_dir + '\\config.yaml'

with open(yaml_file,"r") as f:
    config = yaml.load(f,Loader = yaml.FullLoader)

def remove_handlers():
    '''It removes existing handlers in the file'''
    # We remove existing handlers in the logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        handler.close()

def create_log(path):
    '''Creates a rotating log
    :param path: The path of the rotating log 
    >>> create_log(path):
        logger
    '''
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                        , handlers=[TimedRotatingFileHandler(path,when = 'd', interval = 1, backupCount= 7)]
                        ,level = logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('Rotating Log')
    return logger

# We import the yaml file configuration from the functions_parser module
with open(yaml_file,"r") as f:
    config = yaml.load(f,Loader = yaml.FullLoader)

# I remove existing handlers in the logger
try:
    remove_handlers()

# In case that I get an error when removing handlers I catch it
except Exception as e:
    print('Fallo al quitar los handlers existentes en el programa')
    print(e)

# I try to create a log and I catch it in case that I doesn't work to show the error message
try:
    logger = create_log(log)
except Exception as e:
    print('Failure when trying to create a program log, exiting the program...')
    print(e) 
    time.sleep(30)
    exit
