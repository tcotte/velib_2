# Imports and running findspark
import findspark
import json
from datetime import datetime
from pyspark.sql import functions as F

findspark.init('/home/bigdata/spark-2.4.3-bin-hadoop2.7')
import pyspark
from pyspark import RDD
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json


def process_stream(record, spark):
    columns = ["Station", "Date", "Last update", "Places", "Available_bikes", "Capacity", "Status", "Position"]
    if not record.isEmpty():
        df = spark.createDataFrame(record, columns)
        df = df.filter(df["Station"] < 1000) # delete 1,033 station which is not in Toulouse
        df.show()
        df.write.format(
            'org.elasticsearch.spark.sql'
        ).mode(
            'overwrite' # or .mode('append')
        ).option(
            'es.nodes', 'localhost'
        ).option(
            'es.port', 9200
        ).option(
            'es.resource', '%s/%s' % ('velotoulouse-geo', '_doc'),
        ).save()


sc = SparkContext(appName="PythonSparkStreamingKafka")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 2)  # Creating Kafka direct stream
dks = KafkaUtils.createDirectStream(ssc, ["velib"],
                                    {"metadata.broker.list": "localhost:9092"})

pairs = dks.map(lambda x: (int(x[0]), json.loads(x[1])))

pairs_registered = pairs.map(lambda x: (x[0],
                                        str(datetime.now().strftime("%m/%d/%Y %H:%M")), x[1].get("last_update"),
                                        x[1].get("available_bike_stands"),
                                        x[1].get('available_bikes'), x[1].get('bike_stands'), x[1].get('status'), str(x[1].get('position').get('lat'))+","+
                                        str(x[1].get('position').get('lng'))))

pairs_registered.foreachRDD(lambda rdd: process_stream(rdd, spark))


ssc.start()
ssc.awaitTermination()
