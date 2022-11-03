import datetime
import itertools
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
  
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

def create_header_and_data(start_year,end_year,base_df,attribute):
    # We filter the data for the start and end years
    results_filtered = base_df[(base_df['FirstDateN1'] > datetime.datetime(start_year,1,1,0,0)) \
        & (base_df['FirstDateN1'] < datetime.datetime(end_year,1,1,0,0))]
    # We sort the values from the smallest to the largest of each song at n1 position
    results_filtered = results_filtered.sort_values(by=[f'{attribute}'])
    # We create a cumulative sum
    results_filtered['cumsum_songs'] = results_filtered.reset_index().index
    # we create a percentage based on the cumsum and the total length of the df
    all_songs = len(results_filtered.index)
    results_filtered['cdf'] = results_filtered['cumsum_songs']*100 / all_songs
    data = results_filtered.copy()
    
    # We get the values for the information that we will put into the table for the graph
    qty_n1_artists = results_filtered['Artist'].nunique()
    qty_n1_songs = results_filtered['Title'].nunique()
    songs_per_artist = round(qty_n1_songs / qty_n1_artists,2)
    start_parsing = results_filtered['FirstDateN1'].min()
    end_parsing = results_filtered['LastDateN1'].max()
    mean = round(results_filtered[f'{attribute}'].mean(),2)
    min = round(results_filtered[f'{attribute}'].min(),0)
    max = results_filtered[f'{attribute}'].max()
    
    # We create a list with the information
    header_data = [qty_n1_artists,qty_n1_songs,songs_per_artist,start_parsing,end_parsing,mean,min,max]
    header_columns = ['qty_n1_artists','qty_n1_songs','songs_per_artist','start_parsing','end_parsing','mean','min','max']

    # We create a cdf graph for the overall distribution of the songs
    header = pd.DataFrame(data = [header_data], columns = header_columns)
    #header_n1_songs.columns = list_columns
    return data, header

def density_function_plot(path,header,data,start_year,end_year,attribute):
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
    data[f'{attribute}'].plot.kde(title = f'Prob and Cum (PDF/CDF) Density Functions of N1 Songs and {attribute}',ax=ax1)
  
    # we define the axises
    ax1.set_xlabel(f'N1 Songs {attribute}', fontsize = 16)
    ax1.set_ylabel("PDF Frecuency",color="blue",fontsize=16)
        
    # We create a twin object for two different y-axis on the sample plot
    ax2=ax1.twinx()
        
    # We plot the cdf (cumulative distribution function)
    ax2.plot(data[f'{attribute}'], data['cdf'],color="green")
    # We name the Y axis
    ax2.set_ylabel("% of measurements",color="red",fontsize=16)
        
    # We set ax3 as the current axes 
    plt.sca(ax3)
    # We hide all the axises and borders
    ax3.get_xaxis().set_visible(False)
    ax3.get_yaxis().set_visible(False)
    plt.box(on=None)
    
    header = header.astype(str)
    summary_table = header[['qty_n1_artists','qty_n1_songs','songs_per_artist','start_parsing','end_parsing','mean','min','max']]
    columns_list = header.columns.tolist()
    # We use the dataframe to create a table
    table = plt.table(cellText=summary_table.values,colLabels=columns_list,cellLoc = 'center', rowLoc = 'center',loc='center'\
        ,colWidths=[0.15,0.15,0.15,0.15,0.15,0.1,0.1,0.1]
    )
       
    # We set auto set font size to false to speed up the canvas drawing process
    table.auto_set_font_size(False)    
        
    # We increase the scale of the row height
    table.scale(1, 2)
    # We save and close the figure
    plt.savefig(f"{path}/cdf_{attribute}_{start_year}_{end_year}.jpg")
    plt.close()