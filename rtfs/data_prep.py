#!/usr/bin/python3
import pandas as pd
import os
import mysql.connector
import pymysql
from sqlalchemy import create_engine


"""config = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": "learn_kaf"
        }

connection = mysql.connector.connect(**config)"""


def QueryCsv(file_path, db_uri):
    # with open(file, 'r') as f:
    #  reader = csv.reader(f)
    #  next(reader , None)

    try:
        if not os.path.isdir(file_path):
            return (f"invalid path{file_path}")

        engine = create_engine(f"mysql+pymysql://{db_uri}")

        csv_files = [file for file in os.listdir(file_path) \
                if file.lower().endswith(".csv")]

        if not csv_files:
            return "none found"

        # dataframes = {}
        for csv_file in csv_files:
            folder_path = os.path.join(file_path, csv_file)
            df = pd.read_csv(folder_path, nrows=5)
            # dataframes[csv_file] = df
            # create a new table in MySQL using the DF
            table_name = csv_file.replace('.csv', '')
            df.to_sql(table_name, engine, if_exists='replace', index=False)

        return "table created succesfully"

    except Exception as e:
        return f"Error occured {e}"


config = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": "learn_kaf"
        }

db_uri = pymysql.connect(**config)

file_path = "/home/ruoya/ramana/rtfs/banking_data1"
result = QueryCsv(file_path, db_uri)
print(result)
