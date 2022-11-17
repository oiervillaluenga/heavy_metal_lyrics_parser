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

# QUESTION 1: How long do n1 songs last in charts vs the rest of the songs?
# QUESTION 2: How many positions do n1 songs climb vs rest of the songs?
# QUESTION 3: What are the start positions of n1 songs in charts vs the rest of the songs?

# We create a summary table with one entry per artist and song
summary_all_data = results.groupby(['Artist','Title','EntryDate','EntryPos','OverallPeakPos','OverallTotalWeeks'])['ThisWeekTotalWeeks'].max().reset_index()

# We filter the df for n1 songs
results_n1_data = results.loc[results['ThisWeekPos'] == 1].copy()

# We create a summary table with one entry per artist and song for the n1 songs 
summary_n1_data = results_n1_data.groupby(['Artist','Title','EntryDate','EntryPos','OverallPeakPos','OverallTotalWeeks'])['ThisWeekTotalWeeks'].max().reset_index()

# We create a new column called N1Song to identify the songs that reached N1 from the ones that didn't
summary_n1_data['N1Song'] = 1

# We merge all the results with the n1 songs to be able to compare the n1 population with the rest of the songs
summary_all_data = summary_all_data.merge(summary_n1_data,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_all_data = summary_all_data.loc[:,~summary_all_data.columns.str.contains('_DROP', case=False)]
summary_all_data['N1Song'] = summary_all_data['N1Song'].replace(np.nan,0)

# We calculate the attributes that we want to draw for each song
max_date = results.groupby(['Artist','Title'])[['ParsingDate']].max().reset_index()
max_date = max_date.rename(columns={'ParsingDate':'LastDate'})
bottom_pos = results.groupby(['Artist','Title'])[['ThisWeekPos']].max().reset_index()
bottom_pos = bottom_pos.rename(columns={'ThisWeekPos':'BottomPos'})
top_pos = results.groupby(['Artist','Title'])[['ThisWeekPos']].min().reset_index()
top_pos = top_pos.rename(columns={'ThisWeekPos':'TopPos'})

# We merge the attributes
summary_all_data = summary_all_data.merge(max_date,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_all_data = summary_all_data.loc[:,~summary_all_data.columns.str.contains('_DROP', case=False)]

summary_all_data = summary_all_data.merge(bottom_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_all_data = summary_all_data.loc[:,~summary_all_data.columns.str.contains('_DROP', case=False)]

summary_all_data = summary_all_data.merge(top_pos,how='left',left_on=['Artist','Title'], right_on = ['Artist','Title'],suffixes = ('', '_DROP'))
summary_all_data = summary_all_data.loc[:,~summary_all_data.columns.str.contains('_DROP', case=False)]

summary_all_data['DeltaClimb'] = summary_all_data['BottomPos'] - summary_all_data['TopPos']
# We do a sanity check and we select the songs where the OverallPeakPos and top positions are the same
# It could be that they are not the same because 
# 1) The song peaked before the 1970 or 2010 ranges 
# 2) The peaking point of the song was not captured by the data collection frequency

summary_all_data['DeltaPos'] = summary_all_data['TopPos'] - summary_all_data['OverallPeakPos']
summary_all_data = summary_all_data[summary_all_data['DeltaPos'] == 0] 
summary_all_data['TimeInCharts'] = (summary_all_data['LastDate'] - summary_all_data['EntryDate']) / np.timedelta64(1, 'D')

summary_all_data = summary_all_data[summary_all_data['TimeInCharts'] >= 0]

# We do another sanity check and we select the songs where the beginning and end of songs is not before or after the time range of analysis
before_filter = datetime.datetime.strptime('1970-01-01', '%Y-%m-%d')
after_filter = datetime.datetime.strptime('2019-12-24', '%Y-%m-%d')
summary_all_data['DateFilter'] = 0
summary_all_data.loc[summary_all_data['EntryDate'] < before_filter, 'DateFilter'] = 1
summary_all_data.loc[summary_all_data['LastDate'] > after_filter, 'DateFilter'] = 1
summary_all_data = summary_all_data[summary_all_data['DateFilter'] == 0]

# We do another sanity check and we select the original lenght of the song in charts is bigger than the calcuated time in charts

summary_all_data['DeltaTimeInCharts'] =  summary_all_data['OverallTotalWeeks']*7 - summary_all_data['TimeInCharts']
summary_all_data = summary_all_data[summary_all_data['DeltaTimeInCharts'] >= 0]

summary_all_data = summary_all_data.drop(['DeltaPos'], axis=1)
summary_n1 = summary_all_data[summary_all_data['N1Song'] == 1]
summary_rest = summary_all_data[summary_all_data['N1Song'] == 0]

decades = [[1970,2019],[1970,1979],[1980,1989],[1990,1999],[2000,2009],[2010,2019]]
for start_year, end_year in decades:
    # We analyze the time that the songs stay in the charts for each of the groups
    try:
        data_all = fp.create_data(start_year,end_year,summary_all_data,'EntryDate','TimeInCharts')
        header_all = fp.create_header(start_year,end_year,summary_all_data,'EntryDate','TimeInCharts')
        data_n1 = fp.create_data(start_year,end_year,summary_n1,'EntryDate','TimeInCharts')
        header_n1 = fp.create_header(start_year,end_year,summary_n1,'EntryDate','TimeInCharts')        
        data_rest = fp.create_data(start_year,end_year,summary_rest,'EntryDate','TimeInCharts')
        header_rest = fp.create_header(start_year,end_year,summary_rest,'EntryDate','TimeInCharts')
    except Exception as e:
        print('error when creating dataframes for graph TimeInCharts')
        print(e) 
    
    try: 
        fp.density_function_plot(cf.charts_dir,header_all,data_all,start_year,end_year,'TimeInCharts_all','TimeInCharts')
        fp.density_function_plot(cf.charts_dir,header_n1,data_n1,start_year,end_year,'TimeInCharts_n1','TimeInCharts')
        fp.density_function_plot(cf.charts_dir,header_rest,data_rest,start_year,end_year,'TimeInCharts_rest','TimeInCharts')
    except Exception as e:
        print('error when creating graph for TimeInCharts')
        print(e)   

    # We analyze the positions that songs climb for each of the groups
    try:
        data_all = fp.create_data(start_year,end_year,summary_all_data,'EntryDate','DeltaClimb')
        header_all = fp.create_header(start_year,end_year,summary_all_data,'EntryDate','DeltaClimb')
        data_n1 = fp.create_data(start_year,end_year,summary_n1,'EntryDate','DeltaClimb')
        header_n1 = fp.create_header(start_year,end_year,summary_n1,'EntryDate','DeltaClimb')        
        data_rest = fp.create_data(start_year,end_year,summary_rest,'EntryDate','DeltaClimb')
        header_rest = fp.create_header(start_year,end_year,summary_rest,'EntryDate','DeltaClimb')
    except Exception as e:
        print('error when creating dataframes for graph TimeInCharts')
        print(e) 
    
    try: 
        fp.density_function_plot(cf.charts_dir,header_all,data_all,start_year,end_year,'DeltaClimb_all','DeltaClimb')
        fp.density_function_plot(cf.charts_dir,header_n1,data_n1,start_year,end_year,'DeltaClimb_n1','DeltaClimb')
        fp.density_function_plot(cf.charts_dir,header_rest,data_rest,start_year,end_year,'DeltaClimb_rest','DeltaClimb')
    except Exception as e:
        print('error when creating graph for DeltaClimb')
        print(e)  

    # We analyze the positions that songs start in the charts for each of the groups
    try:
        data_all = fp.create_data(start_year,end_year,summary_all_data,'EntryDate','DeltaClimb')
        header_all = fp.create_header(start_year,end_year,summary_all_data,'EntryDate','DeltaClimb')
        data_n1 = fp.create_data(start_year,end_year,summary_n1,'EntryDate','DeltaClimb')
        header_n1 = fp.create_header(start_year,end_year,summary_n1,'EntryDate','DeltaClimb')        
        data_rest = fp.create_data(start_year,end_year,summary_rest,'EntryDate','DeltaClimb')
        header_rest = fp.create_header(start_year,end_year,summary_rest,'EntryDate','DeltaClimb')
    except Exception as e:
        print('error when creating dataframes for graph TimeInCharts')
        print(e) 
    
    try: 
        fp.density_function_plot(cf.charts_dir,header_all,data_all,start_year,end_year,'DeltaClimb_all','DeltaClimb')
        fp.density_function_plot(cf.charts_dir,header_n1,data_n1,start_year,end_year,'DeltaClimb_n1','DeltaClimb')
        fp.density_function_plot(cf.charts_dir,header_rest,data_rest,start_year,end_year,'DeltaClimb_rest','DeltaClimb')
    except Exception as e:
        print('error when creating graph for DeltaClimb')
        print(e)  


# QUESTION 4: How long do songs last as n1?
# QUESTION 5: How many positions do n1 songs climb from bottom to n1?
# QUESTION 6: How much time does it take for a song to climb from bottom to n1 position?

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
        data = fp.create_data(start_year,end_year,summary_n1_data,'FirstDateN1','DeltaTimeN1')
        header = fp.create_header(start_year,end_year,summary_n1_data,'FirstDateN1','DeltaTimeN1')
    except Exception as e:
        print('error when creating dataframes for graph DeltaTimeN1')
        print(e)               

    try: 
        fp.density_function_plot(cf.charts_dir,header,data,start_year,end_year,'Time at N1','DeltaTimeN1')
    except Exception as e:
        print('error when creating graph for DeltaTimeN1')
        print(e)   

    # We create graphs for the BottomPos attribute
    try:
        data = fp.create_data(start_year,end_year,summary_n1_data,'FirstDateN1','BottomPos')
        header = fp.create_header(start_year,end_year,summary_n1_data,'FirstDateN1','BottomPos')        
    except Exception as e:
        print('error when creating dataframes for graph BottomPos')
        print(e)               

    try:
        fp.density_function_plot(cf.charts_dir,header,data,start_year,end_year,'Positions climbed from bottom to N1','BottomPos')
    except Exception as e:
        print('error when creating graph for BottomPos')
        print(e)  

    # We create graphs for the DeltaTimeClimb attribute
    try:
        data = fp.create_data(start_year,end_year,summary_n1_data,'FirstDateN1','DeltaTimeClimb')
        header = fp.create_header(start_year,end_year,summary_n1_data,'FirstDateN1','DeltaTimeClimb')          
    except Exception as e:
        print('error when creating dataframes for graph DeltaTimeClimb')
        print(e)               

    try:
        fp.density_function_plot(cf.charts_dir,header,data,start_year,end_year,'Time to climb from bottom to N1','DeltaTimeClimb')
    except Exception as e:
        print('error when creating graph for DeltaTimeClimb')
        print(e)  
  