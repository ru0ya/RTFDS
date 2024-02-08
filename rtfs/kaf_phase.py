#!/usr/bin/python3
"""Kafka Phase0 test"""
import psycopg2
import pandas as pd
import time
from psycopg2 import extras


conn = psycopg2.connect(
        dbname="dummy",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
        )

cursor = conn.cursor()


def create_table(cursor):
    """
    Function to create table in postgresql

    Args: cursor - connection point

    REturns: table
    """
    table = "dummy_table"
    cursor.execute(f"""
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table}(
            transaction_id VARCHAR(255),
            bank VARCHAR(50),
            country VARCHAR(50),
            cust_id VARCHAR(255),
            acct_id VARCHAR(255),
            trans_type VARCHAR(50),
            service VARCHAR(255),
            amount VARCHAR(255),
            trans_time VARCHAR(255),
            product VARCHAR(255),
            trans_date VARCHAR(255)
            )"""
        )
    return table


def insert_data(cursor, csv_path, table):
    """
    FUnction that inserts data into postgresdb

    Args: cursor - connection point
          csv_path - path to file
    Returns: success on data insertion
    """
    df = pd.read_csv(csv_path, nrows=10)

    if len(df) > 0:
        df_columns = list(df)

        # create columns
        columns = ",".join(df_columns)

        # create values
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create Insert Into table
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

        with conn.cursor() as cur:
            extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()


if __name__ == "__main__":
    csv_path = "/home/ruoya/ramana/rtfs/banking_data1/core_transactions.csv"
    table_name = create_table(cursor)

    while True:
        insert_data(cursor, csv_path, table_name)
        print(f"Data inserted into table: {table_name}")
        time.sleep(10)
