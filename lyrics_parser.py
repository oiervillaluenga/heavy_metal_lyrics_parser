import yaml
import pandas as pd
import time
import datetime
import os
import numpy as np
from functions import log_conf as cf
from functions import functions_parser as fp
import data_processing as dp
import data_analysis as da

summary_all_data = dp.summary_all_data
summary_n1 = dp.summary_n1
summary_rest = dp.summary_rest

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
  