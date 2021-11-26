# _______________________________________________________
# Reading CSV files into a Dataframe
# _______________________________________________________
from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("JSONFlightApp")
    .enableHiveSupport()
    .getOrCreate())

file = """../flight-delays/data/csv/*"""
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
df = (spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .option("mode", "FAILFAST") #exit if errors
    .option("nullValue", "")
    .load(file))

df.show(15)

# _______________________________________________________
# Reading CSV files into a SQL table
# _______________________________________________________
spark.sql("CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING csv OPTIONS (path '../flight-delays/data/csv/*', header 'true', inferSchema 'true', mode 'FAILFAST')")
spark.sql("SELECT * FROM us_delay_flights_tbl").show(100)

# _______________________________________________________
# Writing dataframe to CSV files
# _______________________________________________________
(df.write.format("csv")
    .mode("overwrite")
    .save("../flight-delays/data/csv/df_csv"))

