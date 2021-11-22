from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("SparkSQLFlightApp")
    .getOrCreate())

csv_file = "./data/departuredelays.csv"

#read and create temporary view
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"

#longest flight distance
spark.sql("""SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""").show(10)

