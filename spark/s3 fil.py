from pyspark.sql.types import *
from pyspark.sql import SparkSession
vishal=SparkSession.builder.master("local[4]").appName("s3").getOrCreate()
data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\20190701.export")
print(data.take(5))