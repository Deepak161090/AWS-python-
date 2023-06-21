"""
@author: ddabral

"""


#import snowflake.connector
import pyodbc
import pandas as pd
import urllib
#import snowflake.connector as snow
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# SQL Server connection details
driver = 'ODBC Driver 17 for SQL Server'
server = 'localhost'
database = ''
trusted_connection = 'yes'
username = ''
password = ''
userId = ''
snowflake_database = ''
snowflake_schema =''

# Create a SQL Server connection
cnxn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection={trusted_connection}")


print('SQL server connected')


ddl=[]

# Connect to SQL Server database
cursor = cnxn.cursor()


# Get list of tables in SQL Server database
cursor.execute("select name from sys.tables t where schema_name(t.schema_id) = 'dbo'")
tables = [row[0] for row in cursor.fetchall()]

print(tables)

# Loop through each table and create a Snowflake table script
for table in tables:
    # Get column information for current table
    #cursor.execute(f"SELECT name FROM sys.COLUMNS where object_id in (select object_id from sys.tables t where schema_name(t.schema_id) = 'raw' and t.name = '{table}'")
    # print(table)
    cursor.execute(f"SELECT name FROM sys.COLUMNS where object_id in (select object_id from sys.tables t where schema_name(t.schema_id) = 'dbo' and t.name = '{table}')")
    columns = [row for row in cursor.fetchall()]

    # Generate Snowflake create table script
    create_table_script = 'CREATE TABLE IF NOT EXISTS ' + snowflake_database + '.' + snowflake_schema + '.' + table + ' ('
    for column in columns:
        create_table_script += column[0] + ' VARCHAR(20000), '
    create_table_script = create_table_script[:-2] + ') ;'
    ddl.append(create_table_script)
    print(create_table_script)

print(ddl)


#creating multiple dataframe
cnxn_str = (f"Driver={driver};"
            f"Server={server};"
            f"Database={database};"
            f"uid={userId};pwd={password};"
            f"Trusted_Connection={trusted_connection}")

connection = pyodbc.connect(cnxn_str)
quoted = urllib.parse.quote_plus(cnxn_str)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')


df={}
for sql_server_table in tables:
    # Get the data from the SQL Server table
    df[sql_server_table] = pd.read_sql("SELECT * FROM {}".format(sql_server_table), engine)
    
print(df)
  
#data into snowflake
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



for t in df.keys():
    df[t].to_sql(name=t, con=engine.connect(), if_exists='append',chunksize=10000, index=False, index_label=None)
