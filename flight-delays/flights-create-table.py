from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .appName("csv_example")
        .getOrCreate())

spark.sql("CREATE DATABASE learn_spark_db")

# spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")
# which is the same as..
csv_file = "./data/departuredelays.csv"

schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# flights_df.show(100)
# #unmanaged table
# (flights_df
#       .write
#       .option("path", "/tmp/data/us_flights_delay")
#       .saveAsTable("us_delay_flights_tbl")).show()

