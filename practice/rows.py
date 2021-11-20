from pyspark.sql import SparkSession
from pyspark.sql import Row

# Row objects can be used to create DataFrames if you need
# them for quick interactivity and exploration

spark = (SparkSession
        .builder
        .appName("rows_example")
        .getOrCreate())

rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])

authors_df.show()