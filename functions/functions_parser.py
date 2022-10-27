import datetime
from datetime import date
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler
import os
import itertools
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd

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

def density_function_plot_n1_songs(path,header,data,start_year,end_year):
    """Create a plot using the header and the data dataframes for n1 songs
    :param path: the path to where the plot will be saved
    :param data: a dataframe that contains all measurement points for all characteristics
    :param header: a dataframe that contains key information for the characteristic that we need to filter the datapoints for   
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
    data['DeltaTimeN1'].plot.kde(title = f'Prob and Cum (PDF/CDF) Density Functions of N1 Songs at Top of the charts',ax=ax1)
  
    # we define the axises
    ax1.set_xlabel("Time a N1 Song Stays at Top of the Charts", fontsize = 16)
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
    
    header = header.astype(str)
    summary_table = header[['qty_n1_artists','qty_n1_songs','avg_n1_songs_per_artist','start_parsing','end_parsing','mean_delta','min_delta','max_delta']]
    columns_list = header.columns.tolist()
    # We use the dataframe to create a table
    table = plt.table(cellText=summary_table.values,colLabels=columns_list,cellLoc = 'center', rowLoc = 'center',loc='center'\
        ,colWidths=[0.1,0.15,0.2,0.1,0.1,0.1,0.1,0.1]
    )
    #,colWidths=[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1])
        
    # We set auto set font size to false to speed up the canvas drawing process
    table.auto_set_font_size(False)    
        
    # We increase the scale of the row height
    table.scale(1, 2)
    # We save and close the figure
    plt.savefig(f"{path}/cdf_N1_songs_{start_year}_{end_year}.jpg")
    plt.close()

def create_header_and_data(start_year,end_year,base_df):
    # We filter the data for the start and end years
    results_n1_agg_filtered = base_df[(base_df['FirstDateN1'] > datetime.datetime(start_year,1,1,0,0)) \
        & (base_df['FirstDateN1'] < datetime.datetime(end_year,1,1,0,0))]
    # We sort the values from the smallest to the largest of each song at n1 position
    results_n1_agg_filtered = results_n1_agg_filtered.sort_values(by=['DeltaTimeN1'])
    # We create a cumulative sum
    results_n1_agg_filtered['cumsum_songs'] = results_n1_agg_filtered.reset_index().index
    # we create a percentage based on the cumsum and the total length of the df
    all_songs = len(results_n1_agg_filtered.index)
    results_n1_agg_filtered['cdf'] = results_n1_agg_filtered['cumsum_songs']*100 / all_songs
    data = results_n1_agg_filtered.copy()
    
    # We get the values for the information that we will put into the table for the graph
    qty_n1_artists = results_n1_agg_filtered['Artist'].nunique()
    qty_n1_songs = results_n1_agg_filtered['TitleArtist'].nunique()
    mean_n1_songs_per_artist = round(qty_n1_songs / qty_n1_artists,2)
    start_parsing = results_n1_agg_filtered['FirstDateN1'].min()
    end_parsing = results_n1_agg_filtered['LastDateN1'].max()
    mean_delta = round(results_n1_agg_filtered['DeltaTimeN1'].mean(),2)
    min_delta = round(results_n1_agg_filtered['DeltaTimeN1'].min(),0)
    max_delta = results_n1_agg_filtered['DeltaTimeN1'].max()
    
    # We create a list with the information
    n1_header_data = [qty_n1_artists,qty_n1_songs,mean_n1_songs_per_artist,start_parsing,end_parsing,mean_delta,min_delta,max_delta]
    list_columns = ['qty_n1_artists','qty_n1_songs','avg_n1_songs_per_artist','start_parsing','end_parsing','mean_delta','min_delta','max_delta']

    # We create a cdf graph for the overall distribution of the songs
    header_n1_songs = pd.DataFrame(data = [n1_header_data], columns = list_columns)
    header_n1_songs.columns = list_columns

    header = header_n1_songs.copy()
    return data, header