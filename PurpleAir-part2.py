import requests
import pandas as pd
from datetime import datetime
import time
import json
import numpy as np
import dask.dataframe as ddf
from pandas import Series, DataFrame
from io import StringIO

key_read  = '4207277A-2EBD-11ED-B5AA-42010A800006'

sleep_seconds = 3


def get_historicaldata(sensors_list,year,bdate,edate,average_time,key_read):
    # Create an empty dataframe
    df1 = pd.DataFrame()
    
    # Historical API URL
    root_api_url = 'https://api.purpleair.com/v1/sensors/'
    
    # Average time: The desired average in minutes, one of the following:0 (real-time),10 (default if not specified),30,60
    average_api = f'&average={average_time}'

    # Creating fields api url from fields list to download the data: Note: Sensor ID/Index will not be downloaded as default
    fields_list = ['pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'humidity_a', 'humidity_b', 
               'temperature_a', 'temperature_b', 'pressure_a', 'pressure_b', 'pm1.0_atm', 'pm1.0_atm_a', 'pm1.0_atm_b', 
                   'pm1.0_cf_1', 'pm1.0_cf_1_a', 'pm1.0_cf_1_b', 'pm2.5_alt', 'pm2.5_alt_a', 'pm2.5_alt_b', 
                   'pm2.5_atm', 'pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 
                   'pm10.0_atm', 'pm10.0_atm_a', 'pm10.0_atm_b', 'pm10.0_cf_1', 'pm10.0_cf_1_a', 
                   'pm10.0_cf_1_b', 'scattering_coefficient', 'scattering_coefficient_a', 'scattering_coefficient_b',
                   'deciviews, deciviews_a', 'deciviews_b', 'visual_range', 'visual_range_a', 
                   'visual_range_b', '0.3_um_count', '0.3_um_count_a', '0.3_um_count_b', 
                   '0.5_um_count', '0.5_um_count_a', '0.5_um_count_b', '1.0_um_count', '1.0_um_count_a',
                   '1.0_um_count_b', '2.5_um_count', '2.5_um_count_a', '2.5_um_count_b', '5.0_um_count', 
                   '5.0_um_count_a', '5.0_um_count_b', '10.0_um_count', '10.0_um_count_a', '10.0_um_count_b']
    for i,f in enumerate(fields_list):
        if (i == 0):
            fields_api_url = f'&fields={f}'
        else:
            fields_api_url += f'%2C{f}'

    # Dates of Historical Data period
    begindate = datetime.strptime(bdate, '%m-%d-%Y')
    enddate   = datetime.strptime(edate, '%m-%d-%Y')
    
    # Downlaod days based on average
    if (average_time == 60):
        date_list = pd.date_range(begindate,enddate,freq='14d') # for 14 days of data
    else:
        date_list = pd.date_range(begindate,enddate,freq='2d') # for 2 days of data
        
    # Converting to UNIX timestamp
    date_list_unix=[]
    for dt in date_list:
        date_list_unix.append(int(time.mktime(dt.timetuple())))

    # Reversing to get data from end date to start date
    date_list_unix.reverse()
    len_datelist = len(date_list_unix) - 1
        
    # Getting 2-data for one sensor at a time
    for s in sensors_list:
        # Adding sensor_index & API Key
        hist_api_url = root_api_url + f'{s}/history/csv?api_key={key_read}'

        # Creating start and end date api url
        for i,d in enumerate(date_list_unix):
            # Wait time 
            time.sleep(sleep_seconds)
            
            if (i < len_datelist):
                print('Downloading for PA: %s for Dates: %s and %s.' 
                      %(s,datetime.fromtimestamp(date_list_unix[i+1]),datetime.fromtimestamp(d)))
                dates_api_url = f'&start_timestamp={date_list_unix[i+1]}&end_timestamp={d}'
            
                # Final API URL
                api_url = hist_api_url + dates_api_url + average_api + fields_api_url
                            
                #
                try:
                    response = requests.get(api_url)
                except:
                    print(api_url)
                #
                try:
                    assert response.status_code == requests.codes.ok
                
                    # Creating a Pandas DataFrame
                    df = pd.read_csv(StringIO(response.text), sep=",", header=0)
                
                except AssertionError:
                    df = pd.DataFrame()
                    print('Bad URL!')
            
                if df.empty:
                    print('------------- No Data Available -------------')
                else:
                    # Adding Sensor Index/ID
                    df['id'] = s
                
                    #
                    date_time_utc=[]
                    for index, row in df.iterrows():
                        date_time_utc.append(datetime.fromtimestamp(row['time_stamp']))
                    df['date_time_utc'] = date_time_utc
                
                    # Dropping duplicate rows
                    df = df.drop_duplicates(subset=None, keep='first', inplace=False)
                    df2 = pd.concat([df1, df], axis = 0)
                    df1 = df2
                    
    df1.to_csv('Yearly_Historical_Data_' + year + '.csv')
    
    


ur_files = ddf.read_csv('sensors_list.csv')
sensors = ur_files.compute()
sensors.head()
average_time = 1440

date_list = list(sensors['date_created'])

sensors_2016 = sensors[sensors['date_created'] < 1451635201]
sensors_2017 = sensors[sensors['date_created'] < 1483257601]
sensors_2018 = sensors[sensors['date_created'] < 1514793601]
sensors_2019 = sensors[sensors['date_created'] < 1546329601]
sensors_2020 = sensors[sensors['date_created'] < 1577865601]
sensors_2021 = sensors[sensors['date_created'] < 1609488001]
sensors_2022 = sensors[sensors['date_created'] < 1641024001]

# Data download period-2016
bdate_2016 = '1-1-2016' 
edate_2016 = '12-31-2016'
sensorList_2016 = list(sensors_2016['sensor_index'])
get_historicaldata(sensorList_2016,'2016',bdate_2016,edate_2016,average_time,key_read)

# Data download period-2017
bdate_2017 = '1-1-2017' 
edate_2017 = '12-31-2017'
sensorList_2017 = list(sensors_2017['sensor_index'])
get_historicaldata(sensorList_2017,'2017',bdate_2017,edate_2017,average_time,key_read)

# Data download period-2018
bdate_2018 = '1-1-2018' 
edate_2018 = '12-31-2018'
sensorList_2018 = list(sensors_2018['sensor_index'])
get_historicaldata(sensorList_2018,'2018',bdate_2018,edate_2018,average_time,key_read)

# Data download period-2019
bdate_2019 = '1-1-2019' 
edate_2019 = '12-31-2019'
sensorList_2019 = list(sensors_2019['sensor_index'])
get_historicaldata(sensorList_2019,'2019',bdate_2019,edate_2019,average_time,key_read)

# Data download period-2020
bdate_2020 = '1-1-2020' 
edate_2020 = '12-31-2020'
sensorList_2020 = list(sensors_2020['sensor_index'])
get_historicaldata(sensorList_2020,'2020',bdate_2020,edate_2020,average_time,key_read)

# Data download period-2022
bdate_2021 = '1-1-2021' 
edate_2021 = '12-31-2021'
sensorList_2021 = list(sensors_2021['sensor_index'])
get_historicaldata(sensorList_2021,'2021',bdate_2021,edate_2021,average_time,key_read)
