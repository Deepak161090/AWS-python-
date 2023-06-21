# -*- coding: utf-8 -*-
"""
Created on Wed May 24 13:58:15 2023

@author: ddabral
"""


from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd


csv_file_path ="C:\\Users\\ddabral\\Downloads\\Fx.csv"
df = pd.read_csv(csv_file_path)
#set your connection to Snowflake -- you can use any DB here with SQLAlchemy
engine = create_engine(URL(
    account = '',
    user = '',
    authenticator = 'externalbrowser',
    database = '',
    schema = '',
    warehouse = '',
    role=''
))
con=engine.connect()

chunksize = 10000 ##how many rows to send at a time
tablename = 'test1' ##what table to write into 

df.to_sql(name=tablename, con=engine.connect(), if_exists='append', index=False, index_label=None, chunksize=chunksize)