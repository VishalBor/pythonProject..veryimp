from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



vishal=SparkSession.builder.master("local[2]").appName("project").getOrCreate()
'''data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\city_revenue\\city_revenue\\city_revenue.txt").map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],int(x[2])))
print(data.take(1))
df=data.toDF(["date","city","rev"])'''
# drop duplicate
'''print(df.select("date","city","rev").count())
print(df.select("date","city","rev").dropDuplicates().count())'''


#Null vales by view/////not working correct it
'''data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\city_revenue\\city_revenue\\city_revenue.txt").map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],int(x[2])))
print(data.take(20))
df=data.toDF(["date","city","rev"])
# df.show()
df.createOrReplaceTempView("myview")
vishal.sql("select * from myview where city is not null").show()'''



# null done by col function
'''from pyspark.sql.functions import *
data2=df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
data2.dropna().show(truncate=False)'''



# Create DataFrame
data = [
    ("James",None,"M"),
    ("Anna","NY","F"),
    ("Julia",None,None)
  ]

columns = ["name","state","gender"]
df = vishal.createDataFrame(data,columns)
df.show()

# Using isNull()
# df.filter("state is NULL").show()
df.filter(df.state.isNull()).show()

df.dropna().show()

'''from pyspark.sql.functions import col
df.filter(col("state").isNull()).show()'''

'''df.filter("state IS NULL AND gender IS NULL").show()
df.filter(df.state.isNull() & df.gender.isNull()).show()

from pyspark.sql.functions import isnull
df.select(isnull(df.state)).show()

# Using isNotNull()
from pyspark.sql.functions import col
df.filter("state IS NOT NULL").show()
df.filter("NOT state IS NULL").show()
df.filter(df.state.isNotNull()).show()
df.filter(col("state").isNotNull()).show()

df.na.drop(subset=["state"]).show()

# Using pySpark SQL
df.createOrReplaceTempView("DATA")
spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()'''

''.