import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from dotenv import load_dotenv
import os

load_dotenv() #environment variables

USERNAME = os.getenv("USERNAME")+"am"
PASSWORD = os.getenv("PASSWORD")
IP_ADDRESS = os.getenv("IP_ADDRESS")
DB_NAME = os.getenv("DB_NAME")
PORT = os.getenv("PORT")
print(USERNAME)
print(PASSWORD)