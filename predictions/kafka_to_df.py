import findspark
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

findspark.init('/home/bigdata/spark-2.4.3-bin-hadoop2.7')
import pyspark
from pyspark import RDD
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json  # Spark context details

def process_stream(record, spark):
    columns = ["Station", "Date", "Available_bikes"]
    if not record.isEmpty():
        df = spark.createDataFrame(record, columns)
        df = df.filter(df["Station"] < 1000)  # delete 1,033 station which is not in Toulouse
        df.show()
        # Write in CSV file
        df.toPandas().to_csv("dataframe_streaming.csv", index=False, header=True)


sc = SparkContext(appName="PythonSparkStreamingKafka")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 60 * 30)
dks = KafkaUtils.createDirectStream(ssc, ["velopredict"], {"metadata.broker.list": "localhost:9092"}).window(300 * 60,
                                                                                                             30 * 60)  # Get 10 last samples all 30 minutes

pairs = dks.map(lambda x: (int(x[0]), json.loads(x[1])))

pairs_registered = pairs.map(lambda x: (x[0],
                                        datetime.fromtimestamp(x[1].get("last_update") / 1000).strftime(
                                            "%m/%d/%Y %H:%M"),
                                        x[1].get('available_bikes')))

pairs_registered.foreachRDD(lambda rdd: process_stream(rdd, spark))

ssc.start()
ssc.awaitTermination()
