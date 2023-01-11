import yaml
import pandas as pd
import time
import datetime
import os
import numpy as np
from scipy.stats import normaltest
from scipy.stats import mannwhitneyu
from functions import log_conf as cf
from functions import functions_parser as fp
from itertools import permutations
from itertools import product
import data_processing as dp

# First we import the data with the features as created in the data processing module
analysis_per_song_artist = dp.analysis_per_song_artist

# We want to create a summary table with data aggregated by decade
def create_summary_per_decade(x):
    '''Starting from dataframe x we create dictionary d, which we will use to create dataframe d
    :param x: dataframe from which to obtain data to create the calculations
    '''
    d = {}
    d['CountSongs'] = x['Title'].count().round(2)
    d['CountN1Songs'] = x['N1Song'].sum().round(2)
    d['SongsPerArtist'] = (x['Title'].count() / x['Artist'].nunique()).round(2)
    d['MeanEntryPos'] = x['EntryPos'].mean().round(2)
    d['MeanBottomPos'] = x['BottomPos'].mean().round(2)   
    d['MeanTopPos'] = x['TopPos'].mean().round(2)
    d['MeanQtyPosClimbed'] = x['QtyPosClimbedTotal'].mean().round(2)
    d['MeanQtyPosClimbedEntry'] = x['QtyPosClimbedEntry'].mean().round(2)
    d['MeanDaysInCharts'] = x['DaysInCharts'].mean().round(2)
    d['MeanDaysToClimbToTop'] = x['DaysToClimbToTop'].mean().round(2)
    d['MeanDaysAtTop'] = x['DaysAtTop'].mean().round(2)    
        
    return pd.Series(d, index=['CountSongs', 'CountN1Songs','SongsPerArtist' \
        ,'MeanEntryPos', 'MeanBottomPos', 'MeanTopPos'\
          ,'MeanQtyPosClimbed',  'MeanQtyPosClimbedEntry'\
            ,'MeanDaysInCharts','MeanDaysToClimbToTop','MeanDaysAtTop'\
                ])

songs_decade = analysis_per_song_artist.groupby('EntryDecade').apply(create_summary_per_decade)

# Once the table has been inspected we want do a number of statistical tests on the data
statistics_columns = ['EntryPos','DaysInCharts','BottomPos','TopPos','QtyPosClimbedTotal','QtyPosClimbedEntry','DaysToClimbToTop','DaysAtTop']
normality_results = []

# NORMALITY TESTS
# null hypothesis: a sample comes from a normal distribution
for entry in statistics_columns:
    data = analysis_per_song_artist[f'{entry}']
    k2, pvalue = normaltest(data)
    normality_results.append(pvalue)

feature_normality = pd.DataFrame([normality_results],columns = statistics_columns)

decades = analysis_per_song_artist['EntryDecade'].unique()
decades = np.sort(decades)

normality_results_decade = []
normality_features = []
normality_decades = []

feature_decade_list = list(product(statistics_columns,decades))

for feature, decade in feature_decade_list:
    data = analysis_per_song_artist[f'{feature}'][analysis_per_song_artist['EntryDecade'] == decade].copy()
    k2, pvalue = normaltest(data)
    normality_results_decade.append(round(pvalue,4))
    normality_features.append(feature)
    normality_decades.append(decade)

normality_df = pd.DataFrame(list(zip(normality_features,normality_decades,normality_results_decade)), columns=['norm_feature','norm_decade','norm_results'])

# EQUIVALENCE BETWEEN DATA PER EACH DECADE
# I create the combination of the decades using the permutations library and I remove the repeated entries
decades = analysis_per_song_artist['EntryDecade'].unique()

permutations_ID = [','.join(i) for i in permutations(decades.astype(str),2)]
permutations_number = [sum(i) for i in permutations(decades,2)] 
permutations_a = [i.split(',')[0] for i in permutations_ID]
permutations_b = [i.split(',')[1] for i in permutations_ID]

permutations_df_prefilter = pd.DataFrame(list(zip(permutations_ID,permutations_number,permutations_a,permutations_b)), columns=['ID','suma','start','end']).sort_values(by=['ID'])
permutations_df_prefilter['start'] = permutations_df_prefilter['start'].astype(int)
permutations_df_prefilter['end'] = permutations_df_prefilter['end'].astype(int)
permutations_df_prefilter['repeated'] = 0
permutations_df_prefilter.loc[permutations_df_prefilter['start'] > permutations_df_prefilter['end'],'repeated'] = 1

permutations_df = permutations_df_prefilter[permutations_df_prefilter['repeated'] == 0]
permutations_df['equivalence'] = 999
eq_start = []
eq_end = []
eq_feature = []
eq_results = []

for start,end in zip(permutations_df['start'].tolist(),permutations_df['end'].tolist()):
    for feature in statistics_columns:
        start_data = analysis_per_song_artist[f'{feature}'][analysis_per_song_artist['EntryDecade'] == start].copy()
        end_data = analysis_per_song_artist[f'{feature}'][analysis_per_song_artist['EntryDecade'] == end].copy()
        U1, p = mannwhitneyu(start_data,end_data)
        eq_start.append(start)
        eq_end.append(end)
        eq_feature.append(feature)
        eq_results.append(round(p,4))

equivalence_df = pd.DataFrame(list(zip(eq_start,eq_end,eq_feature,eq_results)), columns=['eq_start','eq_end','eq_feature','eq_results'])

# Next I would like to know if the same stands true for the n1 songs
# Are n1 songs equivalent across decades for the features that we deem important?

analysis_per_song_artist_n1 = analysis_per_song_artist[analysis_per_song_artist['N1Song'] == 1]

eq_start_n1 = []
eq_end_n1 = []
eq_feature_n1 = []
eq_results_n1 = []
for start,end in zip(permutations_df['start'].tolist(),permutations_df['end'].tolist()):
    for feature in statistics_columns:
        start_data = analysis_per_song_artist_n1[f'{feature}'][analysis_per_song_artist_n1['EntryDecade'] == start].copy()
        end_data = analysis_per_song_artist_n1[f'{feature}'][analysis_per_song_artist_n1['EntryDecade'] == end].copy()
        U1, p = mannwhitneyu(start_data,end_data)
        eq_start_n1.append(start)
        eq_end_n1.append(end)
        eq_feature_n1.append(feature)
        eq_results_n1.append(round(p,4))

equivalence_df_n1 = pd.DataFrame(list(zip(eq_start_n1,eq_end_n1,eq_feature_n1,eq_results_n1))\
    , columns=['eq_start','eq_end','eq_feature','eq_results'])
