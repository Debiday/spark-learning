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
#or written in equivalent Data-Frame API query
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
    .where(col("distance")>1000)
    .orderBy(desc("distance"))).show(100)

#all flights with at least 2-hour delay
spark.sql("""SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC""").show(10)
#equivalant Data-Frame API query
(df.select("date", "delay", "origin", "destination")
    .where(col("delay")>120)
    .where(col("origin")=='SFO')
    .where(col("destination") == 'ORD')
    .orderBy(desc("delay"))).show(100)

#labelling the extent of the delay
spark.sql("""SELECT delay, origin, destination,
            CASE
                WHEN delay > 360 THEN 'Very Long Delays'
                WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                WHEN delay > 0 and delay < 60  THEN  'Tolerable Delays'
                WHEN delay = 0 THEN 'No Delays'
                ELSE 'Early'
            END AS Flight_Delays
            FROM us_delay_flights_tbl
            ORDER BY origin, delay DESC""").show(100)
#equivalant Data-Frame API query?
