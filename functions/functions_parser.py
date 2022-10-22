import datetime
from datetime import date
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler
import os
import itertools
import matplotlib.pyplot as plt
import matplotlib

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

def create_year_range(year):
    '''Creates a time range combining month and year for a period between start and end years
    :return: list
    >>>convertir_fechas(2022-01-01)
    2022-12-31 00:00:00
    '''
    months = range(1,13)
    days = [1,7,13,19,25] 
    year_list = [year]   
    combined_list = list(itertools.product(year_list, months,days))
    return combined_list

def create_month_range(year,month):
    '''Creates a time range combining month and year for a period between start and end years
    :return: list
    >>>convertir_fechas(2022-01-01)
    2022-12-31 00:00:00
    '''
    month = [month]
    days = [1,7,13,19,25] 
    year = [year]   
    combined_list = list(itertools.product(year, month,days))
    return combined_list

def density_function_plot_n1_songs(path,header,data):
    """Create a plot using the header and the data dataframes for n1 songs
    :param path: the path to where the plot will be saved
    :param data: a dataframe that contains all measurement points for all characteristics
    :param header: a dataframe that contains key information for the characteristic that we need to filter the datapoints for   
    >>> density_function_plot(path,header,data) 
    
    """      
    # We set the characteristics of the plot
    gridsize = (4,4)
    fig = plt.figure(figsize = (12.8,9.6))
    plt.Figure(dpi=500)
    # We create 2 axes: ax1 for the top part where it will hold the graph and ax3 for the information table
    ax1 = plt.subplot2grid(gridsize,(0,0),colspan = 4, rowspan = 3)
    ax3 = plt.subplot2grid(gridsize, (3, 0),colspan = 4, rowspan = 1)
        
    # We define ax1 as the current axe on the plot
    plt.sca(ax1)        
        
    # we plot a kde graph based on a dataframe
    data['DeltaTimeN1'].plot.kde(title = f'Prob (PDF) and Cum (CDF) Density Functions of n1 songs',ax=ax1)
  
    # we define the axises
    ax1.set_xlabel("Weeks_n1_songs", fontsize = 16)
    ax1.set_ylabel("PDF Frecuency",color="blue",fontsize=16)
        
    # We create a twin object for two different y-axis on the sample plot
    ax2=ax1.twinx()
        
    # We plot the cdf (cumulative distribution function)
    ax2.plot(data['DeltaTimeN1'], data['cdf'],color="green")
    # We name the Y axis
    ax2.set_ylabel("% of measurements",color="red",fontsize=16)
        
    # We set ax3 as the current axes 
    plt.sca(ax3)
    # We hide all the axises and borders
    ax3.get_xaxis().set_visible(False)
    ax3.get_yaxis().set_visible(False)
    plt.box(on=None)
    
    summary_table = header[['qty_n1_artists','qty_n1_songs','avg_n1_songs_per_artist','start_parsing','end_parsing','mean_delta','min_delta','max_delta']]
    # We use the dataframe to create a table
    table = plt.table(cellText=summary_table,colLabels=summary_table.columns,cellLoc = 'center', rowLoc = 'center',loc='center',colWidths=[0.15,0.15,0.15,0.15,0.15,0.15,0.15,0.15])
        
    # We set auto set font size to false to speed up the canvas drawing process
    table.auto_set_font_size(False)    
        
    # We increase the scale of the row height
    table.scale(1, 2)
    # We save and close the figure
    plt.savefig(f"{path}/plt_kde_N1_songs_cdf_plot.jpg")
    plt.close()
