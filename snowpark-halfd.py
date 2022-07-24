from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.functions import col
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
df_trips = new_session.sql("select TRIPID, STARTTIME, ENDTIME, DURATION FROM TRIPS_VW")
df_trips.show()

dfHalf = df_trips.with_column("halfdistance", call_udf("halfUDFPerm", col("duration")))
dfHalf.show()

dfDistanceRank = dfHalf.with_column("distancerank", call_udf("rateDistancePerm", col("halfdistance")))
dfDistanceRank.show()


new_session.close()

