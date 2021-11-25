# _______________________________________________________
# Notes on DataFrameReader
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

# file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
# 2010-summary.parquet/"""
# df = spark.read.format("parquet").load(file)

