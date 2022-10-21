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
import glob
import numpy as np

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

# I check if there already exists a parquet file in the working directory where the data is stored 
while os.path.isfile(all_data_file) == False:
    #If there isn't we proceed to the data extraction
    try:
        list_years  = [x for x in range(1970,2020)]
        list_months = [x for x in range(1,13)]
        list_years_months = list(itertools.product(list_years, list_months))
        data_extraction_dates  = [fp.create_month_range(x,y) for x,y in list_years_months]
    except Exception as e: 
        print('Failure when trying to create the list of months and years...')
        print(e)
        logger.debug('Failure executing create_time_range')
        logger.debug(f'{e}')
        time.sleep(30)
        exit    

    # we iterate through each year_month combination in the list of years to extract the results of the billboard songs and we append each df to the dictionary
    for date,name in zip(data_extraction_dates,list_years_months):
        # we create an empty dictionary where we will store the dataframes with the billboard song results for each decade
        data_file = current_dir + '\\data' + f'{name[0]}_{name[1]}.parquet'
        if os.path.isfile(data_file) == False:
            df_collection = {}
            start = time.time()
            for year, month, day in date:
                print(f'year:{year} month: {month} day: {day} started')
                url = f'http://www.umdmusic.com/default.asp?Lang=English&Chart=D&ChDay={day}&ChMonth={month}&ChYear={year}&ChBand=&ChSong='
                try: 
                    df_url = pd.read_html(url)[9]
                except Exception as e:
                    print('Error when reading url in pandas')
                    print(e)
                    continue
                df_url = df_url.iloc[2:,:]
                parsing_date = f'{year}-{month}-{day}'
                dt = datetime.datetime.strptime(parsing_date,'%Y-%m-%d')
                df_url['date'] = dt
                result = {parsing_date:df_url}
                df_collection[parsing_date] = df_url
                del df_url

            end = time.time()
            loop_exec_time = end - start
            logger.debug(f'exec time for loop to extract songs data for {name}: {loop_exec_time}')
            start = time.time() 
            # we concatenate the dictionary values (dataframes) into a unique dataframe
            df_results = pd.concat(df_collection.values(),ignore_index=True)
            # we rename the columns
            df_results.columns = ['ThisWeekPos','LastWeekPos','ThisWeekPeakPos','ThisWeekTotalWeeks','TitleArtist','EntryDate','EntryPos','OverallPeakPos','OverallTotalWeeks','ParsingDate']

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
            print(f'year: {name[0]} month: {name[1]} completed')
            del df_results,df_contents_artists,df_contents_songs,df_contents
            

    # we use glob to combine all parquet files into one file in case that a one unique file doesnt exist

    pattern = current_dir + '\\data*' 
    path = glob.glob(pattern)
    frames = [pd.read_parquet(f) for f in path]
    resultant_dataframe = pd.concat(frames)
    resultant_dataframe.to_parquet(all_data_file)
    print('overall agreggated unique file for all results created')

else:
    print('overall agreggated result file already exists')  

#ANALYSIS: CHARACTERIZATION OF N1 SONGS
# We will ge the average time of a n1 song at n1 
results = pd.read_parquet(all_data_file)
results['ThisWeekPos'] = results['ThisWeekPos'].fillna(0).astype(int)
results['ThisWeekPeakPos'] = results['ThisWeekPeakPos'].fillna(0).astype(int)
results['ThisWeekTotalWeeks'] = results['ThisWeekTotalWeeks'].fillna(0).astype(int)
results['OverallPeakPos'] = results['OverallPeakPos'].fillna(0).astype(int)
results['OverallTotalWeeks'] = results['OverallTotalWeeks'].fillna(0).astype(int)

# ¿How long do songs stay in n1?
# We filter the df for n1 songs
type = results.dtypes
results_n1 = results.loc[results['ThisWeekPos'] == 1].copy()

# We get the first and last dates for each song at n1
min_date_n1 = results_n1.groupby(['Artist','Title'])[['ParsingDate']].min()
max_date_n1 = results_n1.groupby(['Artist','Title'])[['ParsingDate']].max()

min_date_n1 = min_date_n1.rename(columns={'ParsingDate':'FirstDateN1'})
max_date_n1 = max_date_n1.rename(columns={'ParsingDate':'LastDateN1'})

# We first merge the min_date results back to results_n1
results_n1 = results_n1.merge(min_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1 = results_n1.loc[:,~results_n1.columns.str.contains('_DROP', case=False)]

# We repeat with max_date results back to results_n1
results_n1 = results_n1.merge(max_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1 = results_n1.loc[:,~results_n1.columns.str.contains('_DROP', case=False)]

# We calculate the Delta: the diff between the first and last times a song reached number1
results_n1['DeltaTimeN1'] = (results_n1['LastDateN1'] - results_n1['FirstDateN1']) / np.timedelta64(1, 'D')
# We create a dataframe with the results
results_n1_agg = results_n1.groupby(['Artist','Title','FirstDateN1','LastDateN1'])['DeltaTimeN1'].max().reset_index()
qty_n1_artists = results_n1_agg['Artist'].nunique()
qty_n1_songs = results_n1[['TitleArtist']].nunique()
mean_n1_songs_per_artist = qty_n1_songs / qty_n1_artists
start_parsing = results_n1['FirstDateN1'].min()
end_parsing = results_n1['LastDateN1'].max()
mean_delta = results_n1['DeltaTimeN1'].mean()
min_delta = results_n1['DeltaTimeN1'].min()
max_delta = results_n1['DeltaTimeN1'].max()
n1_header_data = [qty_n1_artists,qty_n1_songs,mean_n1_songs_per_artist,start_parsing,end_parsing,mean_delta,min_delta,max_delta]
list_columns = ['qty_n1_artists','qty_n1_songs','avg_n1_songs_per_artist','start_parsing','end_parsing','mean_delta','min_delta','max_delta']
# We create a cdf graph for the overall distribution of the songs
header_n1_songs = pd.DataFrame(data = [n1_header_data], columns = list_columns)
header_n1_songs.columns = list_columns
results_n1_agg['cumsum_songs'] = results_n1_agg.reset_index().index
all_songs = len(results_n1_agg.index)
results_n1_agg['cdf'] = results_n1_agg['cumsum_songs']*100 / all_songs

try: 
    fp.density_function_plot_n1_songs(current_dir,header_n1_songs,results_n1_agg)
except Exception as e:
    print('Error trying to create a cdf graph for n1 songs')
    print(e)
    logger.debug(f'{e}')

        

        
