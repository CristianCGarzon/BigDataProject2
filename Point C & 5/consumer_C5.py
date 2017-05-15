from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
try:
    import json
except ImportError:
    import simplejson as json
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 30)
    dStream = KafkaUtils.createDirectStream(context, ["pointc5"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(p1)
    context.start()
    context.awaitTermination()

def insertUsers(users, spark, time):
    if users:
        rddUsers = sc.parallelize(users)
        # Convert RDD[String] to RDD[Row] to DataFrame
        usersDataFrame = spark.createDataFrame(rddUsers.map(lambda x: Row(screenName=x, hora=time)))
        usersDataFrame.createOrReplaceTempView("users")
        usersDataFrame = spark.sql("use bigdata")
        usersDataFrame = spark.sql("select screenName, hora from users limit 50")
        usersDataFrame.write.mode("append").saveAsTable("users")
        print("Inserted Users FINISH")
    else:
        print("Is not Users avaliables to insert in hive")

def insertText(keywords, spark, time):
    if keywords:
        rddKeywords = sc.parallelize(keywords)
        rddKeywords = rddKeywords.map(lambda x: x.split()).flatMap(lambda x: x).map(lambda x: x.lower())
        rddKeywords = rddKeywords.filter(lambda x:  x == "trump" or x == "maga" or x == "dictator" or x == "impeach" or x == "drain" or x == "swamp")
        if rddKeywords.count() > 0:
            # Convert RDD[String] to RDD[Row] to DataFrame
            keywordsDataFrame = spark.createDataFrame(rddKeywords.map(lambda x: Row(keyword=x, hora=time)))
            keywordsDataFrame.createOrReplaceTempView("keywordsSpecific")
            keywordsDataFrame = spark.sql("use bigdata")
            keywordsDataFrame = spark.sql("select keyword, hora, count(*) as total from keywordsSpecific group by keyword, hora")
            keywordsDataFrame.write.mode("append").saveAsTable("keywordsSpecific")
            print("Inserted keywords 5 FINISH")
    else:
        print("Is not keywords 5 avaliables to insert in hive")

def p1(time,rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    records = rdd.collect() #Return a list with tweets
    spark = getSparkSessionInstance(rdd.context.getConf())
    
    #########################################Punto C#########################################
    users = [element["user"]["screen_name"] for element in records if "user" in element]
    insertUsers(users, spark, time)
    #########################################Punto C#########################################

    #########################################Punto 5#########################################
    keywords = [element["text"] for element in records if "text" in element]
    insertText(keywords, spark, time)
    #########################################Punto 5#########################################

if __name__ == "__main__":
    print("Starting to read tweets")
    sc = SparkContext(appName="ConsumerC5")
    consumer()
    