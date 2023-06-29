# import pandera as data validation for dataframe

import pandera as pa
from pandera import Column, DataFrameSchema

# define function for validation string data
def str_data(colname):
    schema = DataFrameSchema(
        columns={
            # make a criteria for string data must be in lowercase and not containing space
            colname: Column(pa.String, pa.Check(lambda s: s.str.islower() | s.str.isspace()))
        }
    )
    return schema

# define function for validation numeric data
def num_data(colname):
    schema = DataFrameSchema(
        columns={
            # make a criteria for numeric data must be in formatted as float number
            colname: Column(pa.Float)
        }
    )
    return schema