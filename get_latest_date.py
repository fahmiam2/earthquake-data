import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from dotenv import load_dotenv
import os

load_dotenv() #environment variables

def latest_date_db():
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

    #get latest data from stored table
    latest_date_db = pd.read_sql('SELECT max(date(time)) as max_date_db FROM indonesia_earthquake_data', con=connection)
    return latest_date_db

