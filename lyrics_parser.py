import yaml
import pandas as pd
import pandas as pd
from functions import functions_parser as fp
import re

# We set up the yaml configuration file
yaml_file = '//srv5/produccion/nerdszone/22.pcdmisparser/config.yaml'
with open(yaml_file,"r") as f:
    config = yaml.load(f,Loader = yaml.FullLoader)

# We remove existing handlers in the logger
try:
    fp.remove_handlers()

# In case that creating the log fails we catch it
except Exception as e:
    print('Fallo al quitar los handlers existentes en el programa')
    print(e)

try:
    logger = fp.create_log(config['logging_path'])
except Exception as e:
    print('Fallo al configurar el log del programa')
    print(e) 

# we use a list comprehension to generate a list of the years between 1970 and 2021
list_years = [x for x in range(1970,1973)]
# we create an empty dictionary where we will store the dataframes with the billboard song results from 1970 to 2021
df_collection = {}

# we iterate through each year in the list of years to extract the results of the billboard songs and we append each df to the dictionary
for year in list_years:
    url = f'http://www.umdmusic.com/default.asp?Lang=English&Chart=D&ChDay=31&ChMonth=12&ChYear={year}&ChBand=&ChSong='
    df_url = pd.read_html(url)[9]
    df_url = df_url.iloc[2:,:]
    result = {year:df_url}
    df_collection[year] = df_url

# we concatenate the dictionary values (dataframes) into a unique dataframe
df_results = pd.concat(df_collection.values(),ignore_index=True)
# we rename the columns
df_results.columns = ['ThisWeekPos','LastWeekPos','PeakPos','TotalWeeks','TitleArtist','EntryDate','EntryPos','PeakPos','TotalWeeks']

# we extract the Artists from the TitleArtist column 
# we will use an empty list where to append the all upper case letters
artists_nested = []
# we iterate through each row
for index, row in df_results.iterrows():
    contents = row['TitleArtist']
    # we create a list to use as a base for the list comprehension
    contents_split = contents.split()
    # we use a list comprehension to select the words that are all uppercase, that have at least an alphabet letter and that are longer than 1
    artist_row = [word for word in contents_split if word.upper() == word and any(c.isalpha() for c in word) == True and len(word) > 1]

    # we append each iteration to the empty list
    artists_nested.append(artist_row)

# we create a dataframe with the nested list
df_artists = pd.DataFrame(artists_nested)

# we join all columns into one and we remove Nan and None values
df_artists['combined'] = df_artists.apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
df_artists['combined'] = df_artists['combined'].str.replace('Nan','',case = False)
df_artists['combined'] = df_artists['combined'].str.replace('None','',case = False)

# we add the artists column to the results dataframe
df_results['Artist'] = df_artists['combined']
df_results['Title'] = [a.replace(b, '') for a, b in zip(df_results['TitleArtist'], df_results['Artist'])]
df_results['Title2'] = [' '.join(set(a.split())-set([b])) for a, b in zip(df_results['TitleArtist'], df_results['Artist'])]
print('ok')