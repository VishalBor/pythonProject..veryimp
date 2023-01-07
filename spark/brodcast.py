import pyspark
from pyspark.sql import SparkSession

vishal = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
'''
states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = vishal.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

rdd = vishal.sparkContext.parallelize(data)

def state_convert(abc):
    return broadcastStates.value[abc]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
print(result)

'''

rddFromFile = vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat",10)
print("TextFile : "+str(rddFromFile.getNumPartitions()))
.