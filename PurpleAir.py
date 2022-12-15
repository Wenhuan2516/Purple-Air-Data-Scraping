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

def get_sensorslist(nwlng,nwlat,selng,selat,location,key_read):
    # PurpleAir API URL
    root_url = 'https://api.purpleair.com/v1/sensors/'

    # Box domain: lat_lon = [nwlng,, nwlat, selng, selat]
    lat_lon = [nwlng, nwlat, selng, selat]
    for i,l in enumerate(lat_lon):
        if (i == 0):
            ll_api_url = f'&nwlng={l}'
        elif (i == 1):
            ll_api_url += f'&nwlat={l}'
        elif (i == 2):
            ll_api_url += f'&selng={l}'
        elif (i == 3):
            ll_api_url += f'&selat={l}'
        
    # Fields to get
    fields_list = ["sensor_index","date_created", "private","name","icon","location_type","model","hardware","latitude","longitude"] 
    for i,f in enumerate(fields_list):
        if (i == 0):
            fields_api_url = f'&fields={f}'
        else:
            fields_api_url += f'%2C{f}'

    # Indoor, outdoor or all
    if (location == 'indoor'):
        loc_api = f'&location_type=1'
    elif (location == 'outdoor'):
        loc_api = f'&location_type=0'
    else:
        loc_api = ''
            
    # Final API URL
    api_url = root_url + f'?api_key={key_read}' + fields_api_url + ll_api_url + loc_api

    # Getting data
    response = requests.get(api_url)

    if response.status_code == 200:
        #print(response.text)
        json_data = json.loads(response.content)['data']
        df = pd.DataFrame.from_records(json_data)
        df.columns = fields_list
    else:
        raise requests.exceptions.RequestException

    # writing to csv file
    filename = 'sensors_list.csv'
    df.to_csv(filename, index=False, header=True)
            
    # Creating a Sensors 
    sensorslist = list(df.sensor_index)
    
    return sensorslist


# A rough boundary of CA
nwlng = -125.858261
nwlat = 49.075363
selng = -65.761040
selat = 25.319627
location = 'outdoor'



sensorList = get_sensorslist(nwlng,nwlat,selng,selat,location,key_read)
    
    


