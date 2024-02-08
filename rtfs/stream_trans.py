#!/usr/bin/python3
"""utilise pyspark on streamed data"""
import pandas as pd
import os
import psycopg2
from time import sleep
from kafka import KafkaProducer
from sqlalchemy import create_engine


def QueryData(filepath, db_uri):
    """
    Read credit card transactions file

    Args: filepath - path to file
          db_uri - dtabase to insert data

    Returns: file data
    """
    try:
        if not os.path.isdir(filepath):
            return f"Invalid path: {filepath}"

        engine = create_engine(db_uri)

        csv_file = [file for file in os.listdir(filepath) if\
                file.lower().endswith(".csv")]

        folder_path = os.path.join(filepath, csv_file)
        df = pd.read_csv(filepath)
        table_name = csv_file.replace('.csv', '')
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        return "Table successfullly created"

    except Exception as e:
        return f"Error occurred: {e}"


config = {
        "host": "localhost",
        "user": "postgres",
        "password": "password",
        "dbname": "dummy"
        }

db_uri = f"postgresql+psycopg2://{config['user']}:\
        {config['password']}@{config['host']}:\
        /{config['dbname']}"


if __name__ == "__main__":
    filepath = "/home/ruoya/ramana_intern/ramana/rtfs/banking_data1/credit_card_transactions.csv"

    result = QueryData(filepath, db_uri)
