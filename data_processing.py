import yaml
import pandas as pd
import time
import datetime
import os
import numpy as np
from functions import log_conf as cf
from functions import functions_parser as fp
import data_creation as dc

# We run and import the results from the data creation module
results = dc.results

# SUMMARY TABLE: ONE ENTRY PER SONG TITLE AND ARTIST
# We remove the entries with no Title or no Artist
results = results.loc[results['Title'].str.len() > 3].copy()
results = results.loc[results['Artist'].str.len() > 3].copy()

results['ThisWeekPos'] = results['ThisWeekPos'].fillna(0).astype(int)
results['ThisWeekPeakPos'] = results['ThisWeekPeakPos'].fillna(0).astype(int)
results['ThisWeekTotalWeeks'] = results['ThisWeekTotalWeeks'].fillna(0).astype(int)
results['OverallPeakPos'] = results['OverallPeakPos'].fillna(0).astype(int)
results['OverallTotalWeeks'] = results['OverallTotalWeeks'].fillna(0).astype(int)
results['EntryPos'] = results['EntryPos'].fillna(0).astype(int)

# We create a summary table with one entry per artist and song and the first time it entered the charts
analysis_per_song_artist = results.groupby(['Artist','Title'])['EntryDate'].min().reset_index()

# We filter the results dataframe by the values that exist in the summary dataframe
results = fp.get_duplicates_df(['Artist','Title','EntryDate'],analysis_per_song_artist,results)

# We create a summary table with one entry per artist and song for the n1 songs 
summary_data = results.groupby(['Artist','Title','EntryDate','EntryPos','OverallPeakPos','OverallTotalWeeks'])['ThisWeekTotalWeeks'].max().reset_index()

# We create a new column called N1Song to identify the songs that reached N1 from the ones that didn't
summary_data['N1Song'] = 0
summary_data.loc[summary_data['OverallPeakPos'] == 1, 'N1Song'] = 1

# We merge all the results with the n1 songs to be able to compare the n1 population with the rest of the songs
analysis_per_song_artist = analysis_per_song_artist.merge(summary_data,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
analysis_per_song_artist = analysis_per_song_artist.loc[:,~analysis_per_song_artist.columns.str.contains('_DROP', case=False)]
analysis_per_song_artist['N1Song'] = analysis_per_song_artist['N1Song'].replace(np.nan,0)

# FEATURE 1: DAYS IN CHARTS

# We calculate the attributes that we want to draw for each song
max_date = results.groupby(['Artist','Title'])[['ParsingDate']].max().reset_index()
max_date = max_date.rename(columns={'ParsingDate':'LastDate'})

analysis_per_song_artist = analysis_per_song_artist.merge(max_date,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
analysis_per_song_artist = analysis_per_song_artist.loc[:,~analysis_per_song_artist.columns.str.contains('_DROP', case=False)]

analysis_per_song_artist['DaysInCharts'] = (analysis_per_song_artist['LastDate'] - analysis_per_song_artist['EntryDate']) / np.timedelta64(1, 'D')

analysis_per_song_artist = analysis_per_song_artist[analysis_per_song_artist['DaysInCharts'] >= 0]

# We do a sanity check and we select the songs where the beginning and end of songs is not before or after the time range of analysis
before_filter = datetime.datetime.strptime('1970-01-01', '%Y-%m-%d')
after_filter = datetime.datetime.strptime('2019-12-24', '%Y-%m-%d')
analysis_per_song_artist['DateFilter'] = 0
analysis_per_song_artist.loc[analysis_per_song_artist['EntryDate'] < before_filter, 'DateFilter'] = 1
analysis_per_song_artist.loc[analysis_per_song_artist['LastDate'] > after_filter, 'DateFilter'] = 1
analysis_per_song_artist = analysis_per_song_artist[analysis_per_song_artist['DateFilter'] == 0]
analysis_per_song_artist = analysis_per_song_artist.drop(['DateFilter'], axis=1)

# We do another sanity check and we select the original length of the song in charts is bigger than the calculated time in charts
analysis_per_song_artist['DeltaTimeInCharts'] =  analysis_per_song_artist['OverallTotalWeeks']*7 - analysis_per_song_artist['DaysInCharts']
analysis_per_song_artist = analysis_per_song_artist[analysis_per_song_artist['DeltaTimeInCharts'] >= 0]
analysis_per_song_artist = analysis_per_song_artist.drop(['DeltaTimeInCharts'], axis=1)

#FEATURE 2: QUANTITY OF POSITIONS CLIMBED FROM BOTTOM TO TOP

top_pos = results.groupby(['Artist','Title'])[['ThisWeekPos']].min().reset_index()
top_pos = top_pos.rename(columns={'ThisWeekPos':'TopPos'})

# For the bottom position, we ensure that the position reflects only the before peak positions
# We get the date of the top positions for n1 songs
date_first_time_n1 = results[results['ThisWeekPeakPos'] == 1].groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
date_first_time_n1 = date_first_time_n1.rename(columns={'ParsingDate':'DateTopPos'})
date_first_time_n1['N1Song_x'] = 1

# We get the dates of the top positions for the no n1 songs
date_top_pos_no_n1 = results[results['OverallPeakPos'] > 1].groupby(['Artist','Title','ParsingDate','OverallPeakPos'])[['ThisWeekPos']].min().reset_index()
date_top_pos_no_n1 = date_top_pos_no_n1.loc[date_top_pos_no_n1['OverallPeakPos'] == date_top_pos_no_n1['ThisWeekPos']].copy()
date_top_pos_no_n1 = date_top_pos_no_n1.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
date_top_pos_no_n1 = date_top_pos_no_n1.rename(columns={'ParsingDate':'DateTopPos'})
date_top_pos_no_n1['N1Song_x'] = 0

# We join both dataframes
date_top_pos = date_first_time_n1.append(date_top_pos_no_n1, ignore_index=True)

# We merge the calculated dataframe to the results dataframe
bottom_pos_results = results.merge(date_top_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
bottom_pos_results = bottom_pos_results.loc[:,~bottom_pos_results.columns.str.contains('_DROP', case=False)]

# We filter the rows that reflect dates prior to the top positions
bottom_pos_results_filtered = bottom_pos_results.loc[bottom_pos_results['ParsingDate'] <= bottom_pos_results['DateTopPos']]

bottom_pos = bottom_pos_results_filtered.groupby(['Artist','Title'])[['ThisWeekPos','EntryPos']].max().reset_index()
bottom_pos['BottomPos'] = bottom_pos[['ThisWeekPos', 'EntryPos']].max(axis=1)
bottom_pos = bottom_pos.drop(['ThisWeekPos','EntryPos'], axis=1)

analysis_per_song_artist = analysis_per_song_artist.merge(bottom_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
analysis_per_song_artist = analysis_per_song_artist.loc[:,~analysis_per_song_artist.columns.str.contains('_DROP', case=False)]

analysis_per_song_artist = analysis_per_song_artist.merge(top_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
analysis_per_song_artist = analysis_per_song_artist.loc[:,~analysis_per_song_artist.columns.str.contains('_DROP', case=False)]

# We do a sanity check and we select the songs where the OverallPeakPos and top positions are the same
# It could be that they are not the same because 
# 1) The song peaked before the 1970 or 2010 ranges 
# 2) The peaking point of the song was not captured by the exact week on which the song was parsed

analysis_per_song_artist['DeltaPos'] = analysis_per_song_artist['TopPos'] - analysis_per_song_artist['OverallPeakPos']
analysis_per_song_artist = analysis_per_song_artist[analysis_per_song_artist['DeltaPos'] == 0] 
analysis_per_song_artist = analysis_per_song_artist.drop(['DeltaPos'], axis=1)
analysis_per_song_artist['QtyPosClimbedTotal'] = analysis_per_song_artist['BottomPos'] - analysis_per_song_artist['TopPos']

#FEATURE 3: QUANTITY OF POSITIONS CLIMBED FROM START TO TOP
analysis_per_song_artist['QtyPosClimbedEntry'] = analysis_per_song_artist['EntryPos'].astype(int) - analysis_per_song_artist['TopPos']

summary_n1 = analysis_per_song_artist[analysis_per_song_artist['N1Song'] == 1]
summary_rest = analysis_per_song_artist[analysis_per_song_artist['N1Song'] == 0]

#FEATURE 4: DAYS TAKEN TO CLIMB FROM START TO TOP POSITION 
bottom_pos_date = bottom_pos_results_filtered.groupby(['Artist','Title'])[['DateTopPos']].max().reset_index()

analysis_per_song_artist = analysis_per_song_artist.merge(bottom_pos_date,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
analysis_per_song_artist = analysis_per_song_artist.loc[:,~analysis_per_song_artist.columns.str.contains('_DROP', case=False)]

analysis_per_song_artist['DaysToClimbToTop'] = (analysis_per_song_artist['DateTopPos'] - analysis_per_song_artist['EntryDate']) / np.timedelta64(1, 'D')

#FEATURE 5: DAYS AT TOP POSITION 
date_top_pos= results.groupby(['Artist','Title','ParsingDate','OverallPeakPos'])[['ThisWeekPos']].min().reset_index()
date_top_pos = date_top_pos.loc[date_top_pos['OverallPeakPos'] == date_top_pos['ThisWeekPos']].copy()

min_date_top_pos= date_top_pos.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
min_date_top_pos = min_date_top_pos.rename(columns={'ParsingDate':'FirstDateTopPos'})

max_date_top_pos= date_top_pos.groupby(['Artist','Title'])[['ParsingDate']].max().reset_index()
max_date_top_pos = max_date_top_pos.rename(columns={'ParsingDate':'LastDateTopPos'})

date_top_pos = min_date_top_pos.merge(max_date_top_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
date_top_pos = date_top_pos.loc[:,~date_top_pos.columns.str.contains('_DROP', case=False)]

date_top_pos['DaysAtTop'] = (date_top_pos['LastDateTopPos'] - date_top_pos['FirstDateTopPos']) / np.timedelta64(1, 'D')
date_top_pos = date_top_pos.drop(['LastDateTopPos','FirstDateTopPos'], axis=1)

analysis_per_song_artist = analysis_per_song_artist.merge(date_top_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
analysis_per_song_artist = analysis_per_song_artist.loc[:,~analysis_per_song_artist.columns.str.contains('_DROP', case=False)]
analysis_per_song_artist.loc[analysis_per_song_artist['DaysAtTop'] < 0,'DaysAtTop'] = 0
analysis_per_song_artist.loc[analysis_per_song_artist['DaysToClimbToTop'] < 0,'DaysToClimbToTop'] = 0

analysis_per_song_artist = analysis_per_song_artist.drop(['ThisWeekTotalWeeks','OverallPeakPos','OverallTotalWeeks'], axis=1)
analysis_per_song_artist['EntryYear'] = analysis_per_song_artist['EntryDate'].dt.year
analysis_per_song_artist['EntryDecade'] = 0

# We get the decades
analysis_per_song_artist.loc[(analysis_per_song_artist['EntryYear'] > 1969) &  (analysis_per_song_artist['EntryYear'] < 1980),'EntryDecade'] = 1970
analysis_per_song_artist.loc[(analysis_per_song_artist['EntryYear'] > 1979) &  (analysis_per_song_artist['EntryYear'] < 1990),'EntryDecade'] = 1980
analysis_per_song_artist.loc[(analysis_per_song_artist['EntryYear'] > 1989) &  (analysis_per_song_artist['EntryYear'] < 2000),'EntryDecade'] = 1990
analysis_per_song_artist.loc[(analysis_per_song_artist['EntryYear'] > 1999) &  (analysis_per_song_artist['EntryYear'] < 2010),'EntryDecade'] = 2000
analysis_per_song_artist.loc[(analysis_per_song_artist['EntryYear'] > 2009) &  (analysis_per_song_artist['EntryYear'] < 2020),'EntryDecade'] = 2010

# We create a summary table with data aggregated by decade
def f(x):
    d = {}
    d['CountSongs'] = x['Title'].count()
    d['CountN1Songs'] = x['N1Song'].sum()
    d['SongsPerArtist'] = x['Title'].count() / x['Artist'].nunique()

    d['MeanEntryPos'] = x['EntryPos'].mean()
    d['MeanBottomPos'] = x['BottomPos'].mean()     
    d['MeanTopPos'] = x['TopPos'].mean()
    
    d['MeanQtyPosClimbed'] = x['QtyPosClimbedTotal'].mean()
    d['MeanQtyPosClimbedEntry'] = x['QtyPosClimbedEntry'].mean()
    
    d['MeanDaysInCharts'] = x['DaysInCharts'].mean()
    d['MeanDaysToClimbToTop'] = x['DaysToClimbToTop'].mean()  
    d['MeanDaysInTop'] = x['DaysInCharts'].mean()    
    


     
    return pd.Series(d, index=['CountSongs', 'CountN1Songs','SongsPerArtist' \
        ,'MeanEntryPos', 'MeanBottomPos', 'MeanTopPos'\
          ,'MeanQtyPosClimbed',  'MeanQtyPosClimbedEntry'\
            ,'MeanDaysInCharts','MeanDaysToClimbToTop','MeanDaysInTop'\
                ])

songs_decade = analysis_per_song_artist.groupby('EntryDecade').apply(f)

