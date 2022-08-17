import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
import pymysql
from get_latest_date import latest_date_db
pymysql.install_as_MySQLdb()

#run get latest date to get recent json data
def get_data():
    latest_date = latest_date_db()
    start_date_df = latest_date + timedelta(days=1)
    start_date = start_date_df['max_date_db'][0]

    yesterday = datetime.now() - timedelta(days=1)
    api = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    payload = {'format' : 'geojson','starttime' : start_date, 'endtime' : yesterday, 
            'maxlatitude' : '6.5', 'minlatitude' : '-11.5',
            'maxlongitude' : '141.5', 'minlongitude' : '94.5', 'minmagnitude' : '2.5'}

    response = requests.get(api, params=payload)
    print(response)
    data_json = response.json()
    # data_json.keys()
    return data_json
    
#convert json data into dataframe
def to_df():
    data_json = get_data()
    data = [quake['properties'] for quake in data_json['features']]
    latitude = []
    longitude = []
    depth = []
    for i, tes in enumerate(data_json['features']):
        coord = tes['geometry']['coordinates']
        #wkwk = pd.DataFrame(tes['properties'], index=[i])
        latitude.append(coord[1])
        longitude.append(coord[0])
        depth.append(coord[2])
    df = pd.DataFrame(data)
    df['latitude'] = latitude
    df['longitude'] = longitude
    df['depth'] = depth
    return df

#cleaning data
def clean_df():
    df = to_df()
    ina_location = ['Indonesia', 'Sumatra', "Andaman Sea", "Arafura Sea", "Bali Sea", "Banda Sea",
        "Celebes Sea", "Flores Sea", "Halmahera Sea", "Java Sea", "Molucca Sea", "Natuna Sea", "Philippine Sea", "Savu Sea", 
        "Seram Sea", "South China Sea", "Timor Sea", "Alas Strait", "Alor Strait", "Badung Strait", "Bali Strait", "Bangka Strait",
        "Berhala Strait", "Dampier Strait", "Gaspar Strait", "Karimata Strait", "Laut Strait", "Lombok Strait", "Madura Strait",
        "Makassar Strait", "Malacca Strait", "Mentawai Strait", "Ombai Strait", "Pitt Strait", "Riau Strait", "Rupat Strait",
        "Sape Strait", "Selayar Strait","Singapore Strait","Sumba Strait","Sunda Strait","Wetar Strait","Ambon Bay","Balikpapan Bay",
        "Berau Gulf","Bintuni Bay","Boni Gulf","Cenderawasih Bay","Jakarta Bay","Lampung Bay","Pelabuhanratu Bay","Saleh Bay","Semangka Bay",
        "Tolo Gulf","Tomini Gulf","Yos Sudarso Bay"
        ]
    df_clean = df[(df['mag']>=2.5) & (df['place'].str.contains('|'.join(ina_location)))].reset_index(drop=True)
    df_clean['date'] = df_clean['time'].apply(lambda d: datetime.datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d'))
    df_clean['time'] = df_clean['time'].apply(lambda d: datetime.datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_clean['location'] = df_clean.place.str.extract(r'\bof\b\s*((?:\w+(?:\S+\b\s*)){1,2})')[0]
    df_clean['location'].loc[df_clean['location'].isnull()] = df_clean.place.str.extract(r'^(.+?),')[0]
    df_clean['location'].loc[(df_clean['location'].isnull() & df_clean.place.str.contains("2004 Sumatra"))] = "Sumatra-Andaman Island"
    print(df_clean)
    return df_clean
