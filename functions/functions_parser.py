import datetime
from datetime import date
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler
import os
import itertools

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
    

def validar_int(check_input):
    '''It checks if the input is a pure digit
    :param check_input: the input to check
    :return: bool    
    >>>validar_int(puede)
    False
    '''
    if check_input.isdigit():
        return int(check_input)
    return -1
  
# Función para validar fechas
def convertir_fechas(date_text):
    '''Converts a data on the '%Y-%m-%d format to a datetime object
    :return: datetime object
    >>>convertir_fechas(2022-01-01)
    2022-12-31 00:00:00
    '''    
    try: 
        fecha = datetime.datetime.strptime(date_text, '%Y-%m-%d') 
        return fecha
    except Exception:
        return False

def create_time_range(start_year,end_year):
    '''Creates a time range combining month and year for a period between start and end years
    :return: list
    >>>convertir_fechas(2022-01-01)
    2022-12-31 00:00:00
    '''
    list_years = range(start_year,end_year)
    months = range(1,13)    
    combined_list = list(itertools.product(list_years, months))
    return combined_list
