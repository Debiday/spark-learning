# _______________________________________________________
# Notes
# _______________________________________________________
#Tables reside within a database. 
#By default, Spark creates tables under the default database.

from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("SparkSQLFlightApp")
    .enableHiveSupport()
    .getOrCreate())

# spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

csv_file = "./data/departuredelays.csv"
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING" 
flights_df = spark.read.csv(csv_file, schema=schema) 
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
