# TODO: Parquet file to DataFrame
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