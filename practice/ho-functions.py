#creating higher-order functions 
#(vs. built in, for full list of built in func see pg.163)

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (SparkSession
    .builder
    .appName("JSONFlightApp")
    .enableHiveSupport()
    .getOrCreate())

schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

t_c.show()

# _______________________________________________________
# transform()
# _______________________________________________________
fahrenheit = spark.sql("""SELECT celsius, transform(celsius, t -> ((t*9) div 5) + 32) as fahrenheit FROM tC""")

fahrenheit.show()

# _______________________________________________________
# filter()
# _______________________________________________________
filtered = spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""")

filtered.show()

# _______________________________________________________
# exists()
# _______________________________________________________
exists = spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as threshold FROM tC""")

exists.show()

#TODO: Fix todo function
# _______________________________________________________
# reduce()
# _______________________________________________________
avg_f = spark.sql("""SELECT celsius, reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC """)
avg_f.show()

