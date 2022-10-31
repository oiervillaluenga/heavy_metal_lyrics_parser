import yaml
import pandas as pd
import time
import datetime
import os
import numpy as np
import data_creation as dc
from functions import functions_parser as fp
from functions import configuration as cf

# We import the results from the data creation module
results = dc.results
results['ThisWeekPos'] = results['ThisWeekPos'].fillna(0).astype(int)
results['ThisWeekPeakPos'] = results['ThisWeekPeakPos'].fillna(0).astype(int)
results['ThisWeekTotalWeeks'] = results['ThisWeekTotalWeeks'].fillna(0).astype(int)
results['OverallPeakPos'] = results['OverallPeakPos'].fillna(0).astype(int)
results['OverallTotalWeeks'] = results['OverallTotalWeeks'].fillna(0).astype(int)

# QUESTION 1: ¿How long do songs last as n1?

# We filter the df for n1 songs
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

# We create a dataframe based on the n1 song results where we calculate de difference between First and Last dates at top of the charts
results_n1_agg = results_n1.groupby(['Artist','Title','TitleArtist','FirstDateN1','LastDateN1'])['DeltaTimeN1'].max().reset_index()
       
decades = [[1970,2019],[1970,1979],[1980,1989],[1990,1999],[2000,2009],[2010,2019]]
for start_year, end_year in decades:
    print(start_year)
    print(end_year)
    try:
        data, header = fp.create_header_and_data(start_year,end_year,results_n1_agg)
    except Exception as e:
        print('error when creating dataframes for graph')
        print(e)               

    try:
        fp.density_function_plot_n1_songs(cf.current_dir,header,data,start_year,end_year)
    except Exception as e:
        print('error when creating graph')
        print(e)   

# QUESTION 2: ¿How much do n1 songs climb to get to n1 (time and positions)?

# We get the first date for each song at n1

results_pre_n1 = results_n1[results_n1["ParsingDate"] <= results_n1["FirstDateN1"] ]

max_pos_pre_n1 = results_pre_n1.groupby(['Artist','Title'])[['ThisWeekPos']].max().reset_index()

print('aaa')