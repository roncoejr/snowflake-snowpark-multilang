from snowflake.snowpark import Session
import os

connection_params = {
    "account": os.environ["SF_ACCOUNT_NAME"],
    "user": "JOHN",
    "password": os.environ["SF_ACCOUNT_PWD"],
    "role": "DBA_CITIBIKE",
    "warehouse": "LOAD_WH",
    "database": "CITIBIKE",
    "schema": "DEMO"
}

new_session = Session.builder.configs(connection_params).create()
df_tables = new_session.sql('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES')
df_tables.show()
new_session.close()

