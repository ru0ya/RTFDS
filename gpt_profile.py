#!/usr/bin/python3
"""
Creating customers profile
"""
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# load from csv
df = pd.read_csv(
        "./banking_data1/credit_card_transactions.csv"
        )

# extract card no, cust id, acct if
customer_profile = df[['card_no', 'cust_id', 'acct_id']].drop_duplicates()

# average transactions per month
df['transaction_date'] = pd.to_datetime(df['transaction_date'])
df['transaction_month'] = df['transaction_date'].dt.month

avg_transactions_per_month = df.groupby(
        ['card_no', 'cust_id', 'acct_id', 'transaction_month']
        ).size().groupby(
                ['card_no', 'cust_id', 'acct_id']
                ).mean().reset_index()

avg_transactions_per_month.rename(
        columns={0: 'avg_transactions_per_month'},
        inplace=True
        )

# calculates avg spent per month
avg_amount_spent_per_month = df.groupby(
        ['cust_id', 'transaction_month', 'transaction_category']
        )['transaction_amount'].mean().astype(float) \
                .rename('avg_spent_per_month').reset_index()

"""
avg_amount_spent_per_month.rename(
        columns={'transaction_amount': 'avg_amount_spent_per_month'},
        inplace=True
        )
"""

# calculates average amount spent per month per category
avg_amount_spent_per_category = df.groupby(
        ['card_no', 'cust_id', 'acct_id', 'transaction_month', 'transaction_category']
        )['transaction_amount'].sum().groupby(
                ['card_no', 'cust_id', 'acct_id', 'transaction_category']
                ).mean().reset_index()

# create table to store customer profile
customer_profile = pd.merge(
        customer_profile,
        avg_transactions_per_month,
        on=['card_no', 'cust_id', 'acct_id'],
        how='left'
        )

customer_profile = pd.merge(
        customer_profile,
        avg_amount_spent_per_month,
        on=['card_no', 'cust_id', 'acct_id'],
        # columns='transaction_category',
        # values='transaction_amount',
        suffixes=('', '_avg'),
        # fill_value=0
        ).reset_index()

# pivot avg_amount_spemt_per_category DataFrame to have categories as columns
avg_amount_spent_per_category = avg_amount_spent_per_category.pivot_table(
        index=['card_no', 'cust_id', 'acct_id'],
        # columns='transaction_category',
        # values='transaction_amount',
        # fill_value=0
        ).reset_index()

# merge the avg_amount_spent_per_category DataFrame with customer_profile DF
customer_profile = pd.merge(
        customer_profile,
        avg_amount_spent_per_category,
        on=['card_no', 'cust_id', 'acct_id'],
        how='left'
        )

# fill missing values with 0
customer_profile.fillna(0, inplace=True)

# db connect
db_config = {
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': 5432,
        'database': 'dummy'
        }

# SQLAlchemy engine
engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

# insert data into postgres table
customer_profile.to_sql(
        'customer_profiles',
        engine,
        if_exists='replace',
        index=False
        )

# close db connection
engine.dispose()

