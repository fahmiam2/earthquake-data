import pandas as pd
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from dotenv import load_dotenv
import os
import main_eq_ina


load_dotenv() #environment variables

USERNAME = os.getenv("USERNAME")+"am"
PASSWORD = os.getenv("PASSWORD")
IP_ADDRESS = os.getenv("IP_ADDRESS")
DB_NAME = os.getenv("DB_NAME")
PORT = os.getenv("PORT")

database_connection = 'mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(USERNAME, PASSWORD, IP_ADDRESS,
                                                                                PORT, DB_NAME)
engine = create_engine(database_connection, echo=False)

#connect to the database
connection = pymysql.connect(host=IP_ADDRESS, user=USERNAME, password=PASSWORD, db=DB_NAME)
#create cursor
cursor = connection.cursor()

#run fetch data
df_clean = main_eq_ina.clean_df()

#export to mysql database
df_clean.to_sql(con=engine, name='indonesia_earthquake_data', if_exists='append', index=False)

#export to google sheet
API_KEY = os.getenv("API_KEY")
GS_KEY = os.getenc("GS_KEY")
gc = gspread.service_account(API_KEY)
sh = gc.open_by_key(GS_KEY)
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, df_clean)
