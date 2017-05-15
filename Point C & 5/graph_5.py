#This code can be tested writting this command spark-submit graph_5.py "2017-05-15 02:00:00"

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import pandas as pan
import matplotlib.pyplot as plot
try:
    import json
except ImportError:
    import simplejson as json
import sys
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--mastern yarn --jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def graph(datetimetest):
    spark = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()
    hashtagQuery = spark.sql("use bigdata")
    query = "SELECT keyword, count(*) as total FROM keywordsspecific WHERE hora BETWEEN (from_utc_timestamp(cast('%s' AS timestamp), 'PT') - interval 1 hour) AND from_utc_timestamp(cast('%s' AS timestamp), 'PT') GROUP BY keyword ORDER BY 2 DESC, 1" % (datetimetest, datetimetest)
    hashtagQuery = spark.sql(query)
    #hashtagQuery.show()
    df = hashtagQuery.toPandas()
    fig = plot.figure(figsize=(13, 13), dpi=100)
    pie=plot.pie( df['total'],labels=df['keyword'],shadow=False, startangle=90, autopct='%1.1f%%')
    df['legend']=df.keyword.astype(str).str.cat(df.total.astype(str), sep=':  ')
    plot.title('Top 10 trending keyword')
    plot.legend(labels=df['legend'], loc="lower left")
    plot.axis('equal')
    plot.tight_layout()
    fig = plot.gcf()
    fig.canvas.set_window_title('Graph point 5')
    plot.subplots_adjust(left=0.18)
    plot.show()

if __name__ == "__main__":
    print("Starting to graph point 5")
    sc = SparkContext(appName="Graph5")
    graph(sys.argv[1])    
