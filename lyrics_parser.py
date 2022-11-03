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

# QUESTION 1: How long do songs last as n1?
# QUESTION 2: How many positions do n1 songs climb from bottom to n1?
# QUESTION 3: How much time does it take for a song to climb from bottom to n1 position?

# We filter the df for n1 songs
results_n1_data = results.loc[results['ThisWeekPos'] == 1].copy()

# We create a summary table with one entry per artist and song
summary_n1_data = results_n1_data.groupby(['Artist','Title','EntryDate','EntryPos','OverallPeakPos','OverallTotalWeeks'])['ThisWeekTotalWeeks'].max().reset_index()

# We get the entry date and the first and last dates for each song at n1
entry_date = results_n1_data.groupby(['Artist','Title'])[['EntryDate']].min().reset_index()
min_date_n1 = results_n1_data.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
max_date_n1 = results_n1_data.groupby(['Artist','Title'])[['ParsingDate']].max().reset_index()

entry_date = entry_date.rename(columns={'EntryDate':'EntryDateN1'})
min_date_n1 = min_date_n1.rename(columns={'ParsingDate':'FirstDateN1'})
max_date_n1 = max_date_n1.rename(columns={'ParsingDate':'LastDateN1'})

# We merge the entry_date results to the summary table
summary_n1_data = summary_n1_data.merge(entry_date,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_n1_data = summary_n1_data.loc[:,~summary_n1_data.columns.str.contains('_DROP', case=False)]

# We merge the min_date results to the summary table
summary_n1_data = summary_n1_data.merge(min_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_n1_data = summary_n1_data.loc[:,~summary_n1_data.columns.str.contains('_DROP', case=False)]

# We repeat with max_date results
summary_n1_data = summary_n1_data.merge(max_date_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_n1_data = summary_n1_data.loc[:,~summary_n1_data.columns.str.contains('_DROP', case=False)]

# We calculate the DeltaTimeN1: the diff between the first and last times a song reached number1
summary_n1_data['DeltaTimeN1'] = (summary_n1_data['LastDateN1'] - summary_n1_data['FirstDateN1']) / np.timedelta64(1, 'D')

#BottomPos: positions climbed by n1 songs from bottom to n1
# First we merge all the results with the min_date_n1 to be able to filter the results for each song prior to getting to n1
results_climb_to_n1 = results.merge(summary_n1_data,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
results_climb_to_n1 = results_climb_to_n1.loc[:,~results_climb_to_n1.columns.str.contains('_DROP', case=False)]

# We filter the results for the parsing dates that are previous to the song reaching n1 and after the entry date of the song
pre_n1 = results_climb_to_n1[results_climb_to_n1["ParsingDate"] <= results_climb_to_n1["FirstDateN1"]]
pre_n1_post_entry_date = pre_n1[pre_n1["ParsingDate"] >= pre_n1["EntryDateN1"]]

# pre_n1_post_entry = pre_n1[pre_n1["EntryDate"] <= results_climb_to_n1["ParsingDate"]]

# We get the maximum position of the song prior to reaching number 1
max_pos_pre_n1 = pre_n1_post_entry_date.groupby(['Artist','Title'])[['ThisWeekPos']].max().reset_index()
max_pos_pre_n1.rename(columns = {'ThisWeekPos':'BottomPos'}, inplace = True)

# We merge the bottom position of the song back to the summary data
summary_n1_data = summary_n1_data.merge(max_pos_pre_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_n1_data = summary_n1_data.loc[:,~summary_n1_data.columns.str.contains('_DROP', case=False)]

#BottomPosDate

# We get the minimum date of the song prior to reaching number 1
min_date_pre_n1 = pre_n1_post_entry_date.groupby(['Artist','Title'])[['ParsingDate']].min().reset_index()
min_date_pre_n1.rename(columns = {'ParsingDate':'BottomPosDate'}, inplace = True)

# We merge the bottom position date of the song back to the summary data
summary_n1_data = summary_n1_data.merge(min_date_pre_n1,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_n1_data = summary_n1_data.loc[:,~summary_n1_data.columns.str.contains('_DROP', case=False)]

summary_n1_data['DeltaTimeClimb'] = (summary_n1_data['FirstDateN1'] - summary_n1_data['BottomPosDate']) / np.timedelta64(1, 'D')

decades = [[1970,2019],[1970,1979],[1980,1989],[1990,1999],[2000,2009],[2010,2019]]
for start_year, end_year in decades:
    # We create graphs for the DeltaTimeN1 attribute
    try:
        data, header = fp.create_header_and_data(start_year,end_year,summary_n1_data,'DeltaTimeN1')
    except Exception as e:
        print('error when creating dataframes for graph DeltaTimeN1')
        print(e)               

    try:
        fp.density_function_plot(cf.current_dir,header,data,start_year,end_year,'DeltaTimeN1')
    except Exception as e:
        print('error when creating graph for DeltaTimeN1')
        print(e)   

    # We create graphs for the BottomPos attribute
    try:
        data, header = fp.create_header_and_data(start_year,end_year,summary_n1_data,'BottomPos')
    except Exception as e:
        print('error when creating dataframes for graph BottomPos')
        print(e)               

    try:
        fp.density_function_plot(cf.current_dir,header,data,start_year,end_year,'BottomPos')
    except Exception as e:
        print('error when creating graph for BottomPos')
        print(e)  

    # We create graphs for the DeltaTimeClimb attribute
    try:
        data, header = fp.create_header_and_data(start_year,end_year,summary_n1_data,'DeltaTimeClimb')
    except Exception as e:
        print('error when creating dataframes for graph DeltaTimeClimb')
        print(e)               

    try:
        fp.density_function_plot(cf.current_dir,header,data,start_year,end_year,'DeltaTimeClimb')
    except Exception as e:
        print('error when creating graph for DeltaTimeClimb')
        print(e)  
  