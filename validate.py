# import libraries

import pandas as pd
import pandera as pa
from datetime import datetime
import json, os

# import from script validating_criteria.py
import validating_criteria

# read the mapping columns from dataframe to assign as string / int / date column
path = os.getcwd()+'/'
f = open(path+'dataframe.json')
dataframe_cols = json.load(f)

# create an empty dataframe to store error log info as dataframe
error_log = pd.DataFrame({})

# get datetime when the function is run
now = pd.to_datetime(datetime.now())

# create function to validating data based on criteria that define using pandera library
def validate_data(df):
    # make error_log as global var
    global error_log

    # create a looping statement for each columns that defined as string column ("col_str") from dataframe.json
    for i in dataframe_cols['abcde']['col_str']:

        # get criteria for string data from function that already defined in validating_criteria.py
        string_column_validate = validating_criteria.str_data(i)      
        try:

            # data validation for each column that mapped to col_str
            string_column_validate.validate(df, lazy=True)

            # print if the data is passed to validate
            print(i+': validation pass')

        # throw an error if the data is not passed after validate
        except pa.errors.SchemaErrors as err:

            # print the column that failed for validate
            print("Schema errors and failure cases: "+i)

            # append error information as dataframe to error log after validate with timestamp and table name
            err.failure_cases['validate_at'] = now
            err.failure_cases['table_name'] = df.name
            error_log = error_log.append(err.failure_cases)

    # create a looping statement for each columns that defined as numeric column ("col_int") from dataframe.json
    for j in dataframe_cols['abcde']['col_int']:
        num_column_validate = validating_criteria.num_data(j)
        try:
            num_column_validate.validate(df, lazy=True)
            # print(j+': validation pass')
        except pa.errors.SchemaErrors as err:
            print("Schema errors and failure cases: "+i)
            err.failure_cases['validate_at'] = now
            err.failure_cases['table_name'] = df.name
            error_log = error_log.append(err.failure_cases)