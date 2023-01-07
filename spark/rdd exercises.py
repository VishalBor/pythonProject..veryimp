from pyspark.sql.types import *
from pyspark.sql import SparkSession

vishal=SparkSession.builder.master("local[1]").appName("rdd_operations").getOrCreate()


data=vishal.sparkContext.textFile("C:\\Users\\Pooja\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\\data\\movies_data\\movies.dat")

mapdata=data.map(lambda x:x.upper())

mapdata1=mapdata.map(lambda x:x.lower())

data1=data.map(lambda x:x.split("::")).map(lambda x:(int(x[0]),x[1],x[2]))

flat=data.flatMap(lambda x:x.split("::"))

filter=flat.filter(lambda x:"Jumanji" in x)

union=data.union(data1)

rdd1 = vishal.sparkContext.parallelize([3, 4, 5, 6, 6, 7, 8, 5, 10])
rdd2 =vishal.sparkContext.parallelize([1, 2, 3, 3, 3, 4, 4, 4, 5])
subtractRDD = rdd1.subtract(rdd2)
print(subtractRDD.collect())

duplicateRDD =vishal.sparkContext.parallelize([1,1,2,2,3,3,3,4,4,4,4])
rdd=duplicateRDD.distinct()
print(rdd.collect())

rdd3 = vishal.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd4=rdd3.sample(False,0.2)
print(rdd4.collect())