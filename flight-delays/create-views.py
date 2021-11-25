from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("SparkSQLFlightApp")
    .getOrCreate())

# csv_file = "./data/departuredelays.csv"
# schema="date STRING, delay INT, distance INT, origin STRING, destination STRING" 
# flights_df = spark.read.csv(csv_file, schema=schema) 
# flights_df.write.saveAsTable("us_delay_flights_tbl")


table = spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (PATH'./data/departuredelays.csv')""")
spark.sql('show databases').show()
table.show(100)

# (flights_df
#         .write
#         .option("path", "./data/departuredelays.csv")
#         .saveAsTable("us_delay_flights_tbl"))

df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")



# df_sfo = spark.sql("SELECT date, delay, origin, destination FROM managed_us_delay_flights_tbl WHERE origin = 'SFO'")
# # df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

df_sfo.show(10)
spark.read.table("us_origin_airport_JFK_tmp_view")