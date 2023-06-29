# etl_postgresql_bigquery

So this project is an example project for my etl process from postgresql to bigquery.

the mechanisms of this project are:
- used bigquery credentials (json file) to access bigquery from python
- used pandera as the library for data validation
- used psycopg2 as the library to make a connection to postgresql
- store all the queries (SQL) to read data from database in a separate files

All the libraries that i used listed in requirements.txt
