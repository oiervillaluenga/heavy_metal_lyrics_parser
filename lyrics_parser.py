import yaml
import pandas as pd
import pandas as pd
from functions import functions_parser as fp
import re
import time
import datetime
import itertools

# I set up the yaml configuration file where I contain variables that I want to be able to change outside of the program
yaml_file = '//srv5/produccion/nerdszone/22.pcdmisparser/config.yaml'
with open(yaml_file,"r") as f:
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
    logger = fp.create_log(config['logging_path'])
except Exception as e:
    print('Failure when trying to create a program log, exiting the program...')
    print(e) 
    time.sleep(30)
    exit

try:
    list_dates = fp.create_time_range(1970,1973)

except Exception as e: 
    print('Failure when trying to create the list of months and years...')
    logger.debug('Failure executing create_time_range')
    logger.debug(f'{e}')
    time.sleep(30)
    exit    

# we create an empty dictionary where we will store the dataframes with the billboard song results from 1970 to 2021
df_collection = {}
start = time.time()
# we iterate through each year in the list of years to extract the results of the billboard songs and we append each df to the dictionary
for year,month in list_dates:
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
logger.debug(f'exec time for loop to extract songs data {loop_exec_time}')
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


