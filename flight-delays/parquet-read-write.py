# _______________________________________________________
# Notes on DataFrameReader: Data source >>> dataframe
# _______________________________________________________
# DataFrameReader.format(args).option("key", "value").schema(args).load()

# Note that you can only access a DataFrameReader through a SparkSession instance. That is, you cannot create an instance of DataFrameReader. To get an instance handle to it, use:
#     SparkSession.read (returns a handle)
#     // or
#     SparkSession.readStream (returns an instance)

# _______________________________________________________
# Notes on DataFrameWriter
# _______________________________________________________
# Unlike with DataFrameReader, you access its instance not from a 
# SparkSession but from the DataFrame you wish to save. 
# DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)

# To get an instance handle, use:
#     DataFrame.write
#     // or
#     DataFrame.writeStream

# _______________________________________________________
# Reading Parquet files into a Dataframe
# _______________________________________________________
from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("SparkSQLFlightApp")
    .enableHiveSupport()
    .getOrCreate())


file = """../flight-delays/data/parquet/2010-summary.parquet/"""
df = spark.read.format("parquet").load(file)

# spark.sql("SELECT * FROM us_delay_flights_tbl").show()
df.show(10)

# _______________________________________________________
# Reading Parquet files into a Spark SQL table (same result)
# _______________________________________________________
spark.sql("CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING parquet OPTIONS (path '../flight-delays/data/parquet/2010-summary.parquet/')")
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# _______________________________________________________
# Writing dataframe to Parquet file
# _______________________________________________________
(df.write.format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
        .save("../flight-delays/data/parquet/df_parquet"))

# _______________________________________________________
# Writing dataframe to Spark SQL table
# _______________________________________________________
(df.write
    .mode("overwrite")
    .saveAsTable("us_delay_flights_tbl"))