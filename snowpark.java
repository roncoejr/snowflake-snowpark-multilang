import com.snowflake.snowpark_java.*;
//import com.snowflake.snowpark_java.types.*;
import java.util.HashMap;
import java.util.Map;
//import com.roncoejr.HalfDuration;

public class snowpark {
    public static void main(String[] args) {
        Map<String, String> properties = new HashMap<>();
        properties.put("URL", System.getenv("SF_URL"));
        properties.put("USER", "JOHN");
        properties.put("PASSWORD", System.getenv("SF_ACCOUNT_PWD"));
        properties.put("ROLE", "DBA_CITIBIKE");
        properties.put("WAREHOUSE", "LOAD_WH");
        properties.put("DB", "CITIBIKE");
        properties.put("SCHEMA", "DEMO");
//        SessionBuilder builder = Session.builder();
        Session session = Session.builder().configs(properties).create();
        session.addDependency("@~/HalfDuration.jar.gz");

        Integer nDesiredRecords = 10;
        if (args.length != 0) {
            nDesiredRecords = Integer.parseInt(args[0]);
        }
        else {
            System.out.println("Getting " + nDesiredRecords + " row(s).");
        }

         DataFrame dfTables = session.sql("select TRIPID, STARTTIME, ENDTIME, DURATION FROM TRIPS_VW");
         dfTables.show(nDesiredRecords);

        long recordCount = dfTables.count();

        String currentDb = session.getCurrentDatabase().orElse("<no current database>");
        System.out.println("Number of records in the DEMO schema in " + currentDb + " database: " + recordCount);

//        HalfDuration halfduration = new HalfDuration();
//         UserDefinedFunction halfDuration = Functions.udf((Integer x) -> x / 2, DataTypes.IntegerType, DataTypes.IntegerType);
//         UserDefinedFunction halfDurationUDF = Functions.udf((String x) -> halfduration.cutInHalf(x), DataTypes.StringType, DataTypes.StringType);

/*          UserDefinedFunction halfDurationUDF = 
            session
                .udf()
                .registerTemporary(
                    "halfUDF",
                    (String x) -> halfduration.cutInHalf(x),
                    DataTypes.StringType,
                    DataTypes.StringType
                );*/


/*                 UserDefinedFunction halfDurationUDF = 
                session
                    .udf()
                    .registerPermanent(
                        "halfUDFPerm",
                        (String x) -> halfduration.cutInHalf(x),
                        DataTypes.StringType,
                        DataTypes.StringType,
                        "~"
                    );

                    
                UserDefinedFunction rateDistanceUDF = 
                session
                    .udf()
                    .registerPermanent(
                        "rateDistancePerm",
                        (String x) -> halfduration.rateDistance(x),
                        DataTypes.StringType,
                        DataTypes.StringType,
                        "~"
                    ); */


        
        // DataFrame dfHalf = dfTables.withColumn("halfDuration", halfDurationUDF.apply(Functions.col("duration")));
        // DataFrame dfHalf = dfTables.withColumn("halfDuration", Functions.callUDF("halfUDF", Functions.col("duration")));
        DataFrame dfHalf = dfTables.withColumn("halfDuration", Functions.callUDF("halfUDFPerm", Functions.col("duration")));
        dfHalf.show(nDesiredRecords);

        DataFrame dfDistanceRank = dfHalf.withColumn("distanceRating", Functions.callUDF("rateDistancePerm", Functions.col("halfDuration")));
        dfDistanceRank.show(nDesiredRecords);

    }
}