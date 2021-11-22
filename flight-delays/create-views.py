from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("SparkSQLFlightApp")
    .getOrCreate())

# df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
# df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")
# # Create a temporary and global temporary view
# df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
# df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# spark.read.table("us_origin_airport_JFK_tmp_view")

# #drop view
# # spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")

# spark.catalog.listDatabases()
# spark.catalog.listTables()
# spark.catalog.listColumns("us_delay_flights_tbl")

# TODO: Figure out how to access the DB in folder