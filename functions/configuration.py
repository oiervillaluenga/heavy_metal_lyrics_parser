import yaml
import pandas as pd
import time
import datetime
import os
import numpy as np
import data_creation as dc
from functions import functions_parser as fp

# We get the current directory 
current_dir = os.getcwd()
all_data_file = current_dir + '\\data.parquet'
log = current_dir + '\\log.txt'

# We import the yaml file configuration from the functions_parser module
with open(fp.yaml_file,"r") as f:
    config = yaml.load(f,Loader = yaml.FullLoader)

# I remove existing handlers in the logger
try:
    fp.remove_handlers()

# In case that I get an error when removing handlers I catch it
except Exception as e:
    print('Fallo al quitar los handlers existentes en el programa')
    print(e)

# I try to create a log and I catch it in case that I doesn't work to show the error message
try:
    logger = fp.create_log(log)
except Exception as e:
    print('Failure when trying to create a program log, exiting the program...')
    print(e) 
    time.sleep(30)
    exit
