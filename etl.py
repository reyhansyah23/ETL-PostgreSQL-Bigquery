# Import libraries

import pandas as pd
import connection
import os, json, pandas_gbq
from google.oauth2 import service_account

from validate import *

# connect to db from connection.py configuration
erkam_conn = connection.Connection().connect_db()

# get table schema for bigquery
path = os.getcwd()+'/'
schema = open(path+'table_schema.json')
data = json.load(schema)

# get credentials for bigquery project
credentials = service_account.Credentials.from_service_account_file('credentials_bigquery.json')

# create a variable for store project dataset and table name from bigquery
destination_raw = 'dataset1.table1'
destination_update = 'dataset1.table2'
destination_error = 'dataset1.error_log'

project_id_bq = 'xxxxxxxxxxxxxxxxx'

# create a function to push dataframe to bigquery
def push_bq(df, destination, data_from_db=True):
    # make an if-else statement to state if the data is data from db or error log
    if data_from_db == True:
        # if the data is data from db it will replace all the data in bigquery table with the latest data from dataframe
        pandas_gbq.to_gbq(df, destination_table=destination,project_id=project_id_bq, if_exists='replace',table_schema=data, credentials=credentials)
    else:
        # if the data is not from db (error log) it will append the data so we can see the oldest data without removing it
        pandas_gbq.to_gbq(df, destination_table='dataset1.error_log',project_id=project_id_bq, if_exists='append', credentials=credentials)

# run an SQL query from newquery.txt file and store to dataframe
with open(path+'newquery.txt', 'r') as file:
    query = str(file.read())
df_raw = pd.read_sql(query,erkam_conn)
df_raw.name = 'df_raw'

# run an SQL query from newquery_update.txt file and store to dataframe
with open(path+'newquery_update.txt', 'r') as file:
    query = str(file.read())
df_update = pd.read_sql(query,erkam_conn)
df_update.name = 'df_update'

# read the mapping columns from dataframe to assign as string / int / date / year column
f = open(path+'dataframe.json')
df_col = json.load(f)

# create a function to transform column based on mapping from code above (dataframe.json)
def transform_df(df):
    """
    - if the column is in 'col_str' it will fill the nan values to blankspace (' ') and make it lowercase
    - if the column is in 'col_date' it change the format to format date '%Y-%m-%d %H:%M:%S' and fill the nan values to blankspace (' ')
    - if the column is in 'col_int' it will fill the nan values to 0 and convert to float datatype
    - if the column is in 'col_year' it will fill the nan values to 0 and replace the 0 to blankspace (' ') and convert to string
    """
    for i in df_col['abcde']['col_str']:
        df[i].fillna(' ', inplace=True)
        df[i] = df[i].str.lower()
    for j in df_col['abcde']['col_date']:
        df[j] = df[j].dt.strftime('%Y-%m-%d %H:%M:%S')
        df[j].fillna(' ',inplace=True)
    for k in df_col['abcde']['col_int']:
        df[k].fillna(0, inplace=True)
        df[k] = df[k].astype(float)
    for l in df_col['abcde']['col_year']:
        df[l] = df[l].fillna(0).astype(int).replace(0,' ')
        df[l] = df[l].astype(str)


# create a looping statement to run transform_df and validate_data for each dataframe that already created from query above
for df_type in [df_raw, df_update]:
    transform_df(df_type)
    validate_data(df_type)


## Push to Bigquery

# push dataframe with different table based on variable destination above 
push_bq(df_raw, destination_raw)
push_bq(df_update, destination_update)

# push error log dataframe
push_bq(error_log,destination=None,data_from_db=False)

# print when the last time script is running
print('\ndata update at: ', pd.to_datetime(datetime.now()))

# print to check total data in df_raw
print('\nraw data rows total: ', df_raw.shape[0])

# print to check total data in df_update
print('update data rows total: ', df_update.shape[0])