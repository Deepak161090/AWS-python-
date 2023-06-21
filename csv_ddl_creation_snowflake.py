# -*- coding: utf-8 -*-
"""
Created on Wed May 24 10:45:11 2023

@author: ddabral
"""

import snowflake.connector
import csv
conn = snowflake.connector.connect(
    user="",
    account="xyz",
    authenticator="externalbrowser",
    warehouse='',
    role=''
)

csv_file_path ="C:\\Users\\ddabral\\Downloads\\Fx.csv"
table_name = ''


def create_table_from_csv(conn, csv_file_path, table_name):

    try:
        # Open the CSV file and read the first row to get the column names
        with open(csv_file_path, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
        
        # Create the DDL statement
        ddl = f"CREATE TABLE {table_name} ("
        for column in header:
            ddl += f"{column} VARCHAR(2000), "
        ddl = ddl[:-2] + ")"
        conn = snowflake.connector.connect(
            user="",
            account="",
            authenticator="",
            warehouse='',
            role=''
        )
        
        # Execute the DDL statement
        cursor = conn.cursor()
        cursor.execute(ddl)
        cursor.close()
        
        print(f"Table {table_name} created successfully")
    except Exception as e:
        # Log the error
        print(f"Error: {e}")
        
create_table_from_csv(conn, csv_file_path, table_name)
