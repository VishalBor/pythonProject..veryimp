from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("handle_corrupted_record").getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("date", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("rev", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True)
])

employee_df = spark.read.option("mode", "PERMISSIVE").schema(schema).option("header", True).option("columnNameOfCorruptRecord", "_corrupt_record").csv("C:\\Users\\Pooja\\Desktop\\spark tasks\\city_revenue\\city_revenue\\city_revenue.txt").show()

from pyspark.sql.functions import col
employee_df.where(col("_corrupt_record").isNull()).drop("_corrupt_record")
 