"""
Created on Fri May 26 14:28:45 2023

@author: iaind
"""

import numpy as np
import pandas as pd

def process_BOA(df_BMU):

    print('process_BOA . . . ')

    # for now, get BOAA data from all files in directory specified.
    df_PN = df_BMU[df_BMU.a == 'PN'].copy().dropna(how='all', axis=1)
    df_PN.columns = ['record_type', 'bm_unit_id', 'sp', 'from_time',
                     'from_level', 'to_time', 'to_level']
    
    df_BOA = df_BMU[df_BMU.a.str.contains('BOA')].copy()
    df_BOA.columns = ['record_type', 'bm_unit_id', 'acceptance_id',
                      'acceptance_time', 'deemed_flag','so_flag',
                      'stor_provider_flag',
                      'rr_inst_flag', 'rr_sched_flag', 'from_time',
                      'from_level', 'to_time', 'to_level']
    # take the time stamp from col D - this is the start of the PN
    df_PN['from_time'] = df_PN['from_time'].map('{:.0f}'.format)
    df_PN.set_index(pd.to_datetime(df_PN['from_time'], format='%Y%m%d%H%M%S'),
                    inplace=True)
    # take the value from col E which is the start time set point
    df_PN = df_PN[['from_level']].astype(float)
    df_PN.columns = ['PN (MW)']

    # get BOA start time
    df_BOA['from_time'] = df_BOA['from_time'].map('{:.0f}'.format)
    df_BOA['from_time'] = pd.to_datetime(df_BOA['from_time'],
                                         format='%Y%m%d%H%M%S')
    df_BOA['to_time'] = df_BOA['to_time'].map('{:.0f}'.format)
    df_BOA['to_time'] = pd.to_datetime(df_BOA['to_time'], format='%Y%m%d%H%M%S')
    
    # Check if next BOA is new or continuation
    df_BOA['dif'] = (df_BOA['acceptance_id'] - df_BOA['acceptance_id'].shift(-1)).abs()
    
    # Preference_logic:
    # if subsequent BOA start time is after previous start time
    df_BOA['overlap'] = (df_BOA['to_time'] > df_BOA['from_time'].shift(-1))
   
    # remove lines where next BOA is new and start supercedes previous
    df_BOA = df_BOA.loc[~((df_BOA.dif==1) & (df_BOA.overlap)),:].copy()
    
    # check for end time going after subsequent start time
    overlap = (df_BOA['to_time'] > df_BOA['from_time'].shift(-1))
    
    # update end time of that BOA as that is where it actually ended
    df_BOA.loc[overlap,
               'to_time'] = df_BOA.loc[overlap.shift(1).bfill(),
                                       'from_time'].values
    
    # create a 1 minute time series to populate with BOA instructions (if any)
    if not df_BOA.empty:
        date_index = (pd.DatetimeIndex([df_BOA.iloc[0, 9],
                                        df_BOA.iloc[-1, 9]]))
        df_BOA_ts = pd.DataFrame([np.nan, np.nan], index=date_index,
                                 columns=['BOA'])
        df_BOA_ts = df_BOA_ts.resample('T').mean()
    else:
        df_BOA_ts = df_BOA.copy()

    for row in range(len(df_BOA)):
        # create a BOA minute time series to populate complete time series
        df_BOA_fill = pd.DataFrame([np.nan, np.nan], index=date_index)
        df_BOA_fill = df_BOA_fill.resample('T').mean()

        # get time start and end and BOA start and end levels
        temp_start = df_BOA.iloc[row, 9]
        BOA_start = df_BOA.iloc[row, 10]
        temp_end = df_BOA.iloc[row, 11]
        BOA_end = df_BOA.iloc[row, 12]

        # update temp BOA dataframe
        df_BOA_fill.loc[temp_start] = BOA_start
        df_BOA_fill.loc[temp_end] = BOA_end

        # get rid of empty points and interpolate between BOAs
        df_BOA_fill = df_BOA_fill.dropna()
        df_BOA_fill = df_BOA_fill.resample('T').mean().interpolate()

        # insert into main BOA time series
        df_BOA_ts.loc[df_BOA_ts.index.isin(df_BOA_fill.index)] = df_BOA_fill

    return df_PN.resample('T').ffill(), df_BOA, df_BOA_ts.resample('T').ffill()

sp  = '2023-03-22'
elexon_name = 'T_CLDCW-1'
api_key = ''

url = ("https://api.bmreports.com:443/BMRS/PHYBMDATA/"
       f"v1?APIKey={api_key}&SettlementDate={sp}&SettlementPeriod=*"
       f"&BMUnitId={elexon_name}&ServiceType=csv")

col_names = ['a','b','c','d','e','f','g','h','i','j','k','l','m']

df_BMU = pd.read_csv(url, skiprows=1, header=None,
                     names=col_names)

df_PN_ts, df_BOA, df_BOA_ts = process_BOA(df_BMU)

    
df_PN_ts.join(df_BOA_ts, how='outer').plot()
