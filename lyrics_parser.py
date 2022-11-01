import yaml
import pandas as pd
import time
import datetime
import os
import numpy as np
from functions import log_conf as cf
from functions import functions_parser as fp
import data_creation as dc


# We import the results from the data creation module
results = dc.results
results['ThisWeekPos'] = results['ThisWeekPos'].fillna(0).astype(int)
results['ThisWeekPeakPos'] = results['ThisWeekPeakPos'].fillna(0).astype(int)
results['ThisWeekTotalWeeks'] = results['ThisWeekTotalWeeks'].fillna(0).astype(int)
results['OverallPeakPos'] = results['OverallPeakPos'].fillna(0).astype(int)
results['OverallTotalWeeks'] = results['OverallTotalWeeks'].fillna(0).astype(int)

# QUESTION 1: ¿How long do songs last as n1?

# We filter the df for n1 songs
results_n1_data = results.loc[results['ThisWeekPos'] == 1].copy()

# We get the first and last dates for each song at n1
min_date_n1 = results_n1_data.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
max_date_n1 = results_n1_data.groupby(['Artist','Title'])[['ParsingDate']].max().reset_index()

min_date_n1 = min_date_n1.rename(columns={'ParsingDate':'FirstDateN1'})
max_date_n1 = max_date_n1.rename(columns={'ParsingDate':'LastDateN1'})

# We first merge the min_date results back to results_n1
results_n1_run = results_n1_data.merge(min_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1_run = results_n1_run.loc[:,~results_n1_run.columns.str.contains('_DROP', case=False)]

# We repeat with max_date results back to results_n1
results_n1_run = results_n1_run.merge(max_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1_run = results_n1_run.loc[:,~results_n1_run.columns.str.contains('_DROP', case=False)]

# We calculate the Delta: the diff between the first and last times a song reached number1
results_n1_run['DeltaTimeN1'] = (results_n1_run['LastDateN1'] - results_n1_run['FirstDateN1']) / np.timedelta64(1, 'D')

# We create a dataframe based on the n1 song results where we calculate de difference between First and Last dates at top of the charts
results_n1_run_agg = results_n1_run.groupby(['Artist','Title','TitleArtist','FirstDateN1','LastDateN1'])['DeltaTimeN1'].max().reset_index()
'''       
decades = [[1970,2019],[1970,1979],[1980,1989],[1990,1999],[2000,2009],[2010,2019]]
for start_year, end_year in decades:
    try:
        data, header = fp.create_header_and_data_time_at_n1(start_year,end_year,results_n1_run_agg)
    except Exception as e:
        print('error when creating dataframes for graph')
        print(e)               

    try:
        fp.density_function_plot_time_at_n1_songs(cf.current_dir,header,data,start_year,end_year)
    except Exception as e:
        print('error when creating graph')
        print(e)   
'''
# QUESTION 2: ¿How much do n1 songs climb to get to n1 (time and positions)?

# We get the first date for each song at n1
min_date_n1 = results_n1_data.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
min_date_n1.rename(columns = {'ParsingDate':'n1_date'}, inplace = True)

# We merge the dataframe with all data with the dataframe that contains the first date of when the song reached number 1
results_climb_to_n1 = results.merge(min_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_climb_to_n1 = results_climb_to_n1.loc[:,~results_climb_to_n1.columns.str.contains('_DROP', case=False)]

# We filter the dataframe with all data for the parsing dates that are previous to the song reaching n1
pre_n1 = results_climb_to_n1[results_climb_to_n1["ParsingDate"] <= results_climb_to_n1["n1_date"] ]

# We get the maximum position of the song prior to reaching number 1
max_pos_pre_n1 = pre_n1.groupby(['Artist','Title'])[['ThisWeekPos']].max().reset_index()
max_pos_pre_n1.rename(columns = {'ThisWeekPos':'BottomPos'}, inplace = True)

# We get the minimum date of the song prior to reaching number 1
min_date_pre_n1 = pre_n1.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
min_date_pre_n1.rename(columns = {'ParsingDate':'BottomDate'}, inplace = True)

# We merge the date at which the song became n1 back to pre_n1
results_n1_climb = pre_n1.merge(min_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1_climb = results_n1_climb.loc[:,~results_n1_climb.columns.str.contains('_DROP', case=False)]

# We merge the bottom position of the song back to results_n1
results_n1_climb = results_n1_climb.merge(max_pos_pre_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1_climb = results_n1_climb.loc[:,~results_n1_climb.columns.str.contains('_DROP', case=False)]

# We merge the bottom date of the song back to results_n1
results_n1_climb = results_n1_climb.merge(min_date_pre_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_n1_climb = results_n1_climb.loc[:,~results_n1_climb.columns.str.contains('_DROP', case=False)]

# We create a dataframe with the aggregation of chart climbing by each song 
results_n1_climb_agg = results_n1_climb.groupby(['Artist','Title','TitleArtist','n1_date','BottomDate','BottomPos'])[['ParsingDate']].min().reset_index()

decades = [[1970,2019],[1970,1979],[1980,1989],[1990,1999],[2000,2009],[2010,2019]]
for start_year, end_year in decades:
    try:
        data, header = fp.create_header_and_data_climb_to_n1(start_year,end_year,results_n1_run_agg)
    except Exception as e:
        print('error when creating dataframes for graph')
        print(e)               

    try:
        fp.density_function_plot_climb_to_n1_songs(cf.current_dir,header,data,start_year,end_year)
    except Exception as e:
        print('error when creating graph')
        print(e)   

