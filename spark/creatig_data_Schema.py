from pyspark.sql.types import *
from pyspark.sql import SparkSession
pooja=SparkSession.builder.master("local[2]").appName("latest_movies").getOrCreate()
data1=pooja.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat").map(lambda x:x.split("::"))
data2=data1.map(lambda x:(int(x[0]),x[1][0:x[1].rfind("(")],int(x[1][x[1].rfind("(")+1:x[1].rfind(")")]),x[2]))
# print(data2.take(5))

# Explicit Schema
schema1=StructType([ \
    StructField("id",IntegerType(),True), \
    StructField("movie",StringType(),True), \
    StructField("year",IntegerType(),True), \
    StructField("genere",StringType(),True) \
    ])

'''df=pooja.createDataFrame(data=data2,schema=schema1)
print(df.printSchema())
print(df.show(5))'''.



# 1. Creating DataFrame by reading CSV file
# it is usefull when schema is available in file
'''df=pooja.read.option("header","true").csv("C:\\Users\\Pooja\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\\data\\movies_data\\movies.dat")
print(df.show())'''


# 1. Creating DataFrame by reading CSV file                                not working ///clear it
# inferSchema creates schema automatically
# df=pooja.read.option("header","true").option("inferSchema","true").tsv("C:\\Users\\Pooja\\Desktop\\spark tasks\various_formats\\various_formats\\movies.tsv")
# print(df.printSchema())


# insert Schema
'''df=pooja.read.option("header","true").schema(schema1).csv("C:\\Users\\Pooja\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\\data\\movies_data\\movies.dat")
print(df.show())'''


# json
'''df=pooja.read.json("C:\\Users\\Pooja\\Desktop\\spark tasks\\various_formats\\various_formats\\m1.json")
print(df.show(5))'''

# parqet
df=pooja.read.parquet("C:\\Users\\Pooja\\Desktop\\spark tasks\\various_formats\\various_formats\\movies.parquet").show(10)
df=pooja.read.json("C:\\Users\\Pooja\\Desktop\\spark tasks\\various_formats\\various_formats\\m1.json").show(5)
