from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

spark = (SparkSession
    .builder
    .appName("UDFtest")
    .enableHiveSupport()
    .getOrCreate())

def cubed(s):
    if s == 0:
        return "hola this this null"
    return s * s * s

#register udf
spark.udf.register("cubed", cubed, LongType())

#generate temporary view
spark.range(1,9).createOrReplaceTempView("udf_test")

#query cubed udf
queried = spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test")

queried.show()