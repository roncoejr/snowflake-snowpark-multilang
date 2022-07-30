from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType
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


halfDurationUDF = udf(lambda x: x / 2, return_type = StringType(), input_types = [StringType()])

halfDurationUDF = udf(lambda x: x / 2, return_type = StringType(), input_types = [StringType()], name = "halfDuration", replace = True)

@udf(name = "halfDuration", is_permanent = True, stage_location = "@~", replace = True)



new_session = Session.builder.configs(connection_params).create()
df_trips = new_session.sql("select TRIPID, STARTTIME, ENDTIME, DURATION FROM TRIPS_VW")
df_trips.show()

dfHalf = df_trips.with_column("halfdistance", call_udf("halfUDFPerm", col("duration")))
dfHalf.show()

dfDistanceRank = dfHalf.with_column("distancerank", call_udf("rateDistancePerm", col("halfdistance")))
dfDistanceRank.show()


new_session.close()

