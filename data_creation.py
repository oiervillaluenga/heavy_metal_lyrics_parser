
import pandas as pd
import pandas as pd
import time
import datetime
import itertools
import os
import glob
from functions import functions_parser as fp
from functions import log_conf as cf

# I check if there already exists a parquet file in the working directory where the data is stored 
while os.path.isfile(cf.all_data_file) == False:
    #If there isn't we proceed to the data extraction
    try:
        list_years  = [x for x in range(1970,2020)]
        list_months = [x for x in range(1,13)]
        list_years_months = list(itertools.product(list_years, list_months))
        data_extraction_dates  = [fp.create_month_range(x,y) for x,y in list_years_months]
    except Exception as e: 
        print('Failure when trying to create the list of months and years...')
        print(e)
        cf.logger.debug('Failure executing create_time_range')
        cf.logger.debug(f'{e}')
        time.sleep(30)
        exit    

    # we iterate through each year_month combination in the list of years to extract the results of the billboard songs and we append each df to the dictionary
    for date,name in zip(data_extraction_dates,list_years_months):
        # we create an empty dictionary where we will store the dataframes with the billboard song results for each decade
        data_file = cf.current_dir + '\\data' + f'{name[0]}_{name[1]}.parquet'
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
            cf.logger.debug(f'exec time for loop to extract songs data for {name}: {loop_exec_time}')
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
            cf.logger.debug(f'exec time for loop to transform songs data for {name}: {loop_exec_time}')
            df_results.to_parquet(data_file)
            print(f'year: {name[0]} month: {name[1]} completed')
            del df_results,df_contents_artists,df_contents_songs,df_contents
            

    # we use glob to combine all parquet files into one file in case that a one unique file doesnt exist

    pattern = cf.current_dir + '\\data*' 
    path = glob.glob(pattern)
    frames = [pd.read_parquet(f) for f in path]
    resultant_dataframe = pd.concat(frames)
    resultant_dataframe.to_parquet(cf.all_data_file)
    print('overall agreggated unique file for all results created')

else:
    cf.logger.debug('trying things with logger')
    print('overall agreggated result file already exists')

results = pd.read_parquet(cf.all_data_file)