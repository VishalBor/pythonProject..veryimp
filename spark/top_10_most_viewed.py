from pyspark.sql.types import *
from pyspark.sql import SparkSession
vishal=SparkSession.builder.master("local[2]").appName("movies").getOrCreate()
data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat")
data1=data.map(lambda x:x.split("::")[1])
# print(data1.take(5))



# problem in ratings do solve
ratings=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\\data\\ratings_data\\ratings.dat")
print(ratings.ta).