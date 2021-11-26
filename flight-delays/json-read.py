# _______________________________________________________
# Reading JSON files into a Dataframe
# _______________________________________________________
from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("JSONFlightApp")
    .enableHiveSupport()
    .getOrCreate())

file = """../flight-delays/data/json/*"""
df = spark.read.format("json").load(file)

df.show(100)

# _______________________________________________________
# Reading JSON files into a SQL table
# _______________________________________________________
spark.sql("CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING json OPTIONS (path '../flight-delays/data/json/*')")
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# _______________________________________________________
# Writing dataframe to Parquet file
# _______________________________________________________
# (df.write.format("json")
#     .mode("overwrite")
#     .option("compression", "snappy")
#     .save("../flight-delays/data/json/df_json1"))


(df.write.json("../flight-delays/data/json/df_json1"))


