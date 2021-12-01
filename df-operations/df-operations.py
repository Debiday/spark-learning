# _______________________________________________________
# Unions, joins and windowing
# _______________________________________________________
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = (SparkSession
    .builder
    .appName("JSONFlightApp")
    .enableHiveSupport()
    .getOrCreate())

tripdelaysFilePath = "./data/departuredelays.csv"
airportsnaFilePath = "./data/airport-codes-na.txt"

#create temp view of airports data 
airportsna = (spark.read
                .format("csv")
                .options(header="true", inferSchema="true", sep="\t")
                .load(airportsnaFilePath))

airportsna.createOrReplaceTempView("airports_na")
airportsna.show()

#create temp view of flight delays
departureDelays =  (spark.read
                        .format("csv")
                        .options(header="true")
                        .load(tripdelaysFilePath))

departureDelays = (departureDelays
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")

departureDelays.show()

#create temporary small table
foo = (departureDelays
        .filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0""")))

foo.createOrReplaceTempView("foo")

foo.show()

# _______________________________________________________
# union: join two different Dataframes with the same schema
# _______________________________________________________
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar") 

#create duplicate to test
joined = bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0"""))

joined.show()

# _______________________________________________________
# join
# _______________________________________________________
airports = airportsna

foo.join(
    airports,
    airports.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
    FROM foo f
    JOIN airports_na a
        ON a.IATA =  f.origin
""").show()

# _______________________________________________________
# adding new columns
# _______________________________________________________
foo2 = (foo.withColumn(
            "status",
            expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
        ))

foo2.show()

# _______________________________________________________
# dropping new columns
# _______________________________________________________
foo3 = foo2.drop("delay")
foo3.show()

# _______________________________________________________
# renaming columns
# _______________________________________________________
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

