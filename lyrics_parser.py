import yaml
import pandas as pd
import pandas as pd
from functions import functions_parser as fp
import re
import time
import datetime
import itertools
import os
import tqdm

# We get the current directory 
current_dir = os.getcwd()
data_file = current_dir + '\\data.parquet'
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

# I check if there already exists a parquet file in the working directory where the data is stored 
if os.path.isfile(data_file) == False:
    #If there isn't we proceed to the data extraction
    try:
        decade1 = fp.create_time_range(1970,1980)
        decade2 = fp.create_time_range(1980,1990)
        decade3 = fp.create_time_range(1990,2000)
        decade4 = fp.create_time_range(2000,2010)
        decade5 = fp.create_time_range(2010,2020)
        list_decades = [decade1,decade2,decade3,decade4,decade5]
        names_decades = ['1971-1980','1981-1990','1991-2000','2001-2010','2011-2020']
    except Exception as e: 
        print('Failure when trying to create the list of months and years...')
        print(e)
        logger.debug('Failure executing create_time_range')
        logger.debug(f'{e}')
        time.sleep(30)
        exit    

    # we iterate through each year in the list of years to extract the results of the billboard songs and we append each df to the dictionary
    for decade,name in zip(list_decades,names_decades):
        # we create an empty dictionary where we will store the dataframes with the billboard song results for each decade
        data_file = current_dir + '\\data' + f'{name}.parquet'
        df_collection = {}
        start = time.time()
        for year,month in decade:
            print(f'year:{year} and month: {month} started')
            url = f'http://www.umdmusic.com/default.asp?Lang=English&Chart=D&ChDay=28&ChMonth={month}&ChYear={year}&ChBand=&ChSong='
            df_url = pd.read_html(url)[9]
            df_url = df_url.iloc[2:,:]
            parsing_date = f'{year}-{month}-28'
            dt = datetime.datetime.strptime(parsing_date,'%Y-%m-%d')
            df_url['date'] = dt
            result = {parsing_date:df_url}
            df_collection[parsing_date] = df_url

        end = time.time()
        loop_exec_time = end - start
        logger.debug(f'exec time for loop to extract songs data for {name}: {loop_exec_time}')
        start = time.time() 
        # we concatenate the dictionary values (dataframes) into a unique dataframe
        df_results = pd.concat(df_collection.values(),ignore_index=True)
        # we rename the columns
        df_results.columns = ['ThisWeekPos','LastWeekPos','PeakPos','TotalWeeks','TitleArtist','EntryDate','EntryPos','PeakPos','TotalWeeks','ParsingDate']

        # we convert the EntryDate column to datetime
        df_results['EntryDate'] =  pd.to_datetime(df_results['EntryDate'])

        # we split and expand the TitleArtist column into each of its words and we fill the na values with 0
        df_contents = df_results['TitleArtist'].str.split(' ',expand = True)
        df_contents = df_contents.fillna(0)

        # we select the song titles, which will be either words with capital letters or single letter words
        df_contents_songs = df_contents.applymap(lambda x: x if x != 0 and (x.upper() != x or len(x) == 1) else 0)

        # we combine all columns into one and we remove both 0s and leading and trailing spaces
        df_contents_songs['combined'] = df_contents_songs.apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        df_contents_songs['combined'] = df_contents_songs['combined'].str.replace('0','',case = False)
        df_contents_songs['combined'] = df_contents_songs['combined'].str.lstrip()
        df_contents_songs['combined'] = df_contents_songs['combined'].str.rstrip()

        # we select the artists, which will be upper case words and that contain at least one word of the alphabet and that have a length of more than 1
        df_contents_artists = df_contents.applymap(lambda x: x if x != 0 and (x.upper() == x and any(c.isalpha() for c in x) == True and len(x) > 1) else 0) 

        # we combine all columns into one and we remove both 0s and leading and trailing spaces
        df_contents_artists['combined'] = df_contents_artists.apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        df_contents_artists['combined'] = df_contents_artists['combined'].str.replace('0','',case = False)
        df_contents_artists['combined'] = df_contents_artists['combined'].str.lstrip()
        df_contents_artists['combined'] = df_contents_artists['combined'].str.rstrip()

        # we add the results of the parsing as an Artist and Title column
        df_results['Artist'] = df_contents_artists['combined']
        df_results['Title'] = df_contents_songs['combined']
        
        end = time.time()
        loop_exec_time = end - start
        logger.debug(f'exec time for loop to transform songs data for {name}: {loop_exec_time}')
        df_results.to_parquet(data_file)
        print(f'decade: {name} completed')
    


