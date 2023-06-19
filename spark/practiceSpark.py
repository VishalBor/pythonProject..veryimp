from pyspark.sql.types import *
from pyspark.sql import functions
from pyspark.sql import SparkSession

# vishal=SparkSession.builder.master("local[2]").appName("spark Practice").getOrCreate()
#
# data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\city_revenue\\city_revenue\\cr_all")
# print(data.take(1))
# data1=data.map(lambda x:x.split(",")).map(lambda x:(int(x[0]),x[1],float(x[2]),x[3],x[4]))
# print(data1.take(1))
# # data2=data1.flatMap(lambda x:x)
# # print(data2.take(5))

# dffilter=data1.filter(lambda x:"surat" in x)
# print(dffilter.take(5))
# dffilter1=data1.filter(lambda x:x[2]>900)
# print(dffilter1.take(5))

# rdd1=vishal.sparkContext.parallelize([1,4,7,8,5,2,3,6,9])
# rdd2=vishal.sparkContext.parallelize([1,5,9,7,5,3])
# union=rdd1.union(rdd2)
# print(union.collect())

# intersection=rdd1.intersection(rdd2)
# print(intersection.collect())

# substract=rdd1.subtract(rdd2)
# print(substract.collect())


# distint=rdd1.distinct()
# print(distint.collect())


# data2=data1.map(lambda x:(x[1],float(x[2])))
# print(data2.take(5))
# kvrdd=data2.groupByKey()
# result=kvrdd.map(lambda x:(x[1])+sum(float(x[2])))
# print(result.collect())


# from pyspark.sql.types import *
# from pyspark.sql import SparkSession
# vishal=SparkSession.builder.master("local[4]").appName("Generes").getOrCreate()
# data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat")
# data1=data.map(lambda x:x.split("::")).map(lambda x:x[2]).flatMap(lambda x:x.split("|"))
# data2 =data1.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+1)
# print(data2.take(9))


# from pyspark.sql.types import *
# from pyspark.sql import SparkSession
# vishal=SparkSession.builder.master("local[4]").appName("Generes").getOrCreate()
# a1=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat")
# print(a1.collect())
# a2=a1.map(lambda x:x.split("::"))
# print(a2.take(2))
# a3=a2.map(lambda x:x[2])
# print(a3.take(1))
# a4=a3.flatMap(lambda x :x.split("|"))
# print(a4.take(5))
# a5=a4.map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y))
# print(a5)
# a6=a5.sortByKey(False)
# print(a6.take(5))


# >> > crData = sc.textFile("file:///path/to/file")
# >> > kvRDD = cityRDD.map(lambda x: x.split(",")).map(lambda x: (x[1], float(x[2])))
# >> > grpRDD = kvRDD.groupByKey()
# >> > aggRDD = grpRDD.map(lambda x: (x[0], sum(x[1])))
# >> > aggRDD.collect()


# kvRDD = cityRDD.map(lambda x : x.split(",")).map(lambda x : (x[1],float(x[2])))
# 	>>> res = aggRDD.reduceByKey(lambda final,sum : final+sum)
# 	>>> res.collect()



# data2=data1.map(lambda x:(int(x[0]),x[1][0:x[1].rfind("(")],int(x[1][x[1].rfind("(")+1:x[1].rfind(")")]),x[2]))
# print(data2.take(5))


# print(a1.take(5))
# a2=a1.map(lambda x:x.split("::"))
# a3=a2.map(lambda x:(int(x[0]),x[1][0:x[1].rfind("(")],int(x[1][x[1].rfind("(")+1:x[1].rfind(")")]),x[2]))
# print(a3.take(5))
# df=a3.toDF(["id","movie","year","genere"])
# print(df.show())
# print(df.printSchema())



# creating DataFrame by providing Explicitly  providing Schema
# schema=StructType([\
#     StructField("id",IntegerType(),True),
#     StructField("movie",StringType(),True),
#     StructField("year",IntegerType(),True),
#     StructField("genere",StringType(),True)])
#
# df=vishal.createDataFrame(schema=schema,data=a3)
# print(df.show(4))
# data=vishal.read.option("header","true").csv("C:\\Users\\Pooja\\Desktop\\spark tasks\\city_revenue\\city_revenue\\cr_all")
# print(data.show(5))
# print(data.printSchema())


# df1=df.select("movies","year")
# df2=df.select(df.movie,df.id,df.year)
# print(df1.show(2))
# print(df2.show(5))
#
from pyspark.sql.functions import col
# df3=df.select(col("id")),col("movie"),col("year")
#
# df4=df.select(df.columns[0:2]).show(3)
#
# print(df.printSchema())
# df.withColumn("id",col("id").cast("string"))
# print(df.printSchema())




data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# df = spark.createDataFrame(data=data, schema = columns)
# print(df.printSchema())
# df.withColumn("salary",col("salary").cast("Integer"))

# df.withColumn("salary",col("salary").cast("Integer")).show()
# df.withColumn("dob",col("dob").cast("Integer"))

# df.withColumn("dob",col("dob").cast("Integer")).show()
#
# print(df.printSchema())
#
# df.withColumn("salary",col("salary")*100).show()
# df.withColumn("Exceed",col("salary")*-1).show()

# from pyspark.sql.functions import lit
# df.withColumn("country",lit("State")).show()

# df.withColumn("gender","sex").show()


from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType

data = [
  (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
  (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
  (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
  (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
  (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
  (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
  StructField('name', StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)
  ])),
  StructField('languages', ArrayType(StringType()), True),
  StructField('state', StringType(), True),
  StructField('gender', StringType(), True)
])

# df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show(truncate=False)
#
# df.select("name.firstname").filter(df.state=="OH").show()
# df1=df.select("name.firstname","name.lastname","state","gender").filter(df.state!="OH").show()
# df2=df.select("name","state","gender").filter(df.gender!="M")
# print(df2.show(5))

# df3=df.select("name","state","languages","gender") & (df.state =="OH").filter(df.gender=="M")
# print(df3.show(5))


# list=["OH","NY"]
# df.filter(df.state.isin(list)).show()
# df.filter(df.state.startswith("N")).show()
# df.filter(df.state.contains("H")).show()
# df1=df.select("name","state","Languages","gender").filter(df.name.like("%rose%")).show()
# df.filter(df.name.like("%rose%")).show()



# Import pySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

# Create DataFrame
# columns= ["name", "dept", "salary"]
# df = spark.createDataFrame(data = data, schema = columns)
# df.printSchema()
# df.show(truncate=False)

# df1=df.distinct()
# print(df1.show())

# df1=df.dropDuplicates()
# print(df1.show())

# df1=df.dropDuplicates(["department","Salary"])
# print(df1.show())


# simpleData = [("James","Sales","NY",90000,34,10000), \
#     ("Michael","Sales","NY",86000,56,20000), \
#     ("Robert","Sales","CA",81000,30,23000), \
#     ("Maria","Finance","CA",90000,24,23000), \
#     ("Raman","Finance","CA",99000,40,24000), \
#     ("Scott","Finance","NY",83000,36,19000), \
#     ("Jen","Finance","NY",79000,53,15000), \
#     ("Jeff","Marketing","CA",80000,25,18000), \
#     ("Kumar","Marketing","NY",91000,50,21000) \
#   ]
# columns= ["name","dept","state","salary","age","bonus"]
# df = spark.createDataFrame(data = simpleData, schema = columns)
# df.printSchema()
# df.show(truncate=False)

# fd=df.select("name","dept","salary").filter(df.salary > 80000).sort("salary")
# print(fd.show(5))

# fd1=df.select("name","dept","state","salary").filter(df.state=="CA").orderBy(df.salary.desc())
# print(fd1.show(10))




emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]
#
# empDF = spark.createDataFrame(data=emp, schema = empColumns)
# empDF.printSchema()
# empDF.show(truncate=False)
#
# dept = [("Finance",10), \
#     ("Marketing",20), \
#     ("Sales",30), \
#     ("IT",40) \
#   ]
# deptColumns = ["dept_name","dept_id"]
# deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)


# join=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"Left").select(empDF.emp_id,empDF.name)

# join=empDF.join(deptDF,empDF.emp_dept_id ==deptDF.dept_id,"left").show()


# joinddf=empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").select(empDF.emp_id,deptDF.dept_name)
# print(joinddf.show(6))

# join=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"leftsemi").show()
# joinDF=empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"leftanti").show()


#
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
# address = [(1,"14851 Jeffrey Rd","DE"),
#     (2,"43421 Margarita St","NY"),
#     (3,"13111 Siemon Ave","CA")]
# df =spark.createDataFrame(address,["id","address","state"])
# df.show()
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace
# df.withColumn("address",regexp_replace("address","Rd","road")).show()
# df.withColumn("address",when(df.address.endswith("Rd"),regexp_replace(df.address,"Rd","r")).when(df.address.endswith("St"),regexp_replace(df.address,"St","street"))).show()


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

# from pyspark.sql.functions import Window
# from pyspark.sql.functions import row_number
# windowSpec  = Window.partitionBy("department").orderBy("salary")


# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
# windowSpec  = Window.partitionBy("department").orderBy("salary")
#
# df.withColumn("row_number",row_number().over(windowSpec)) \
#     .show(truncate=False)

# from pyspark.sql.functions import Window

# from pyspark.sql.functions import rank
# windowspec=Window.partitionBy("department").orderBy("salary")
# df1=df.withColumn("row_number Col",row_number().over(windowspec)).show()
# df2=df.withColumn("ranking col",rank().over(windowspec)).show()

# from pyspark.functions.functions import dense_rank
# windowSpec= Window.partationBy("department").orderBy("dept")
# df1=df.withColumn("denserank",dense_rank().over(windowspec)).show()


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col
windowSpec  = Window.partitionBy("department").orderBy("salary")

# df.withColumn("row_number",row_number().over(windowSpec)) \
#     .show(truncate=False)

"""dens_rank"""
from pyspark.sql.functions import dense_rank
df1=df.withColumn("dense_rank",dense_rank().over(windowSpec)).filter("dense_rank==2") \
    .show()
# df2=df1.filter(col("dense_rank") == 2)