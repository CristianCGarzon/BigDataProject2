#This code can be tested writting this command spark-submit graph_C.py "2017-05-15 02:00:00"

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
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master yarn --jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def graph(datetimetest):
    spark = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()
    hashtagQuery = spark.sql("use bigdata")
    query = "SELECT screenname, count(*) as total FROM users WHERE hora BETWEEN (from_utc_timestamp(cast('%s' AS timestamp), 'PT') - interval 12 hour) AND from_utc_timestamp(cast('%s' AS timestamp), 'PT') GROUP BY screenname ORDER BY 2 DESC LIMIT 10" % (datetimetest, datetimetest)
    hashtagQuery = spark.sql(query)
    #hashtagQuery.show()
    df = hashtagQuery.toPandas()
    fig = plot.figure(figsize=(13, 13), dpi=100)
    pie=plot.pie( df['total'],labels=df['screenname'],shadow=False, startangle=90, autopct='%1.1f%%')
    df['legend']=df.screenname.astype(str).str.cat(df.total.astype(str), sep=':  ')
    plot.title('Top 10 trending screenname')
    plot.legend(labels=df['legend'], loc="lower left")
    plot.axis('equal')
    plot.tight_layout()
    fig = plot.gcf()
    fig.canvas.set_window_title('Graph point C')
    plot.subplots_adjust(left=0.18)
    plot.show()

if __name__ == "__main__":
    print("Starting to graph point C")
    sc = SparkContext(appName="GraphC")
    graph(sys.argv[1])    
