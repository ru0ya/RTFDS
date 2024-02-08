#!/usr/bin/python3
"""stream data"""
import psycopg2
import pandas as pd
from psycopg2 import extras
from time import sleep


conn = psycopg2.connect(
        dbname="dummy",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
        )

cursor = conn.cursor()


def createTable(cursor):
    """
    Function to create table in postgresqldb

    Args: cursor - connection point

    Returns: table
    """
    table = "cardTrans"
    cursor.execute(f"""
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table}(
            cust_id VARCHAR(50),
            acct_id VARCHAR(250),
            card_no VARCHAR(50),
            transaction_date VARCHAR(250),
            transaction_amount VARCHAR(250),
            transaction_category VARCHAR(50),
            country VARCHAR(50),
            terminal_id VARCHAR(250),
            mode_of_payment VARCHAR(50),
            terminal_source VARCHAR(50),
            transaction_id VARCHAR(250),
            reward VARCHAR(50),
            reward_perc VARCHAR(50)
        ) """
        )

    return table


def insert_data(cursor, csv_path, table):
    """
    Function that creates and inserts data in postgresdb

    Args: - cursor: connection point
          - csv_path: path to file

    Returns: successful if inserted
    """
    df = pd.read_csv(csv_path, nrows=40)

    if len(df) > 0:
        df_columns = list(df)

        # create columns
        columns = ",".join(df_columns)

        # create values
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create, Insert into table
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

        with conn.cursor() as cur:
            extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()


if __name__ == "__main__":
    csv_path = "/home/ruoya/ramana_intern/ramana/rtfs/banking_data1/credit_card_transactions.csv"
    table_name = createTable(cursor)

    while True:
        insert_data(cursor, csv_path, table_name)
        print(f"Data inserted {table_name} successfully")
        sleep(10)
