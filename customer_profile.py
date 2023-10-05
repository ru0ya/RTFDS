#!/usr/bin/python3
"""creating a customer profile from credit card transactions"""
import pandas as pd
import psycopg2
import random
from sqlalchemy import create_engine


conn = psycopg2.connect(
        database="dummy",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
        )

# cursor = conn.cursor()

engine = create_engine('postgresql://postgres:password@localhost/dummy')

cursor = conn.cursor()


df = pd.read_csv(
        "./banking_data1/credit_card_transactions.csv"
        )[:500]

# randomly select a sample of rows
"""
sample_size = 1000
random.seed(42)
selected_rows = random.sample(range(len(df)), sample_size)
df = df.iloc[selected_rows]
"""
column_data_type = df.dtypes
print(column_data_type)

# cleaning the dataset
# handlse missing values, none found
missing_values = df.isnull().sum()
# print(missing_values)

# check for duplicates
df_dropped = df.dropna()

# find duplicates
duplicates = df.duplicated()
# print(duplicates)


# print(df[:10])

# extracts card_no, cust_id and acct_id
profile = df[['card_no', 'cust_id', 'acct_id']].drop_duplicates()

# print(profile)

# convert transaction_date column to datetime
df['transaction_date'] = pd.to_datetime(df['transaction_date'])

# extract the month from transaction_date
df['month'] = df['transaction_date'].dt.month

# group data by month and customer and count the transactions
trans_per_month = df.groupby(['cust_id', 'month'])['transaction_id'].count()

# reset the index
trans_per_month = trans_per_month.reset_index()
# print(f'Transactions p/m: {trans_per_month}')
# print(df.dtypes)

#group dataframe by cust_id
grouped = trans_per_month.groupby('cust_id')

# calculates sum of transaction counts per customer
sum_trans_count = grouped['transaction_id'].sum()

# calculates average count of transactions per customer
# find number of months in dataset
num_months = trans_per_month['month'].nunique()
avg_customer_trans = sum_trans_count / num_months

print("number of months is:")
print(num_months)

print("average customer transactions:")
print(avg_customer_trans)

print("sum of transaction count")
print(sum_trans_count)
# print(avg_customer_trans)

# calculates amount spent per month
amt_tran = df.pivot_table(
        index=['cust_id', 'month'],
        values='transaction_amount',
        aggfunc='sum'
        )
# reset the index
amt_tran = amt_tran.reset_index()

# average of amount spent per month for each customer
avg_spent = df.pivot_table(
        index=['cust_id', 'month'],
        values='transaction_amount',
        aggfunc='mean'
        )

# reset vindex
avg_spent = avg_spent.reset_index()

print("transacted amount:", amt_tran)
print("average spent:", avg_spent)

# average spent per month per category
avg_cat = df.groupby(['cust_id', 'month', 'transaction_category'])['transaction_amount'].mean().astype(float)
# reset index
avg_cat = avg_cat.reset_index()

print(avg_cat)
# customer_profile.fillna(0, inplace=True)
# print(df.columns.tolist())

table_name = "customer_profile"
customer_profile = pd.DataFrame({
    'card_no': df['card_no'],
    'cust_id': df['cust_id'],
    'acct_id': df['acct_id'],
    'avg_customer_trans': avg_customer_trans,
    'amt_tran': amt_tran['transaction_amount'],
    'avg_spent': avg_spent['transaction_amount'],
    'avg_cat': avg_cat['transaction_amount']
    })

# data = df[columns]
customer_profile.fillna(0, inplace=True)

table = customer_profile.to_sql(
        table_name,
        engine,
        if_exists='replace',
        index=False,
        method='multi'
        )

# conn.close()

print(table)

cursor.execute(f""" SELECT * FROM {table_name}""")

data = cursor.fetchall()

for row in data:
    print(row)

conn.close()
