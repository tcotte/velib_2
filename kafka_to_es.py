# Imports and running findspark
import findspark
import json
from datetime import datetime

findspark.init('/home/bigdata/spark-2.4.3-bin-hadoop2.7')
import pyspark
from pyspark import RDD
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json  # Spark context details


def process_stream(record, spark):
    columns = ["Station", "Date", "Last update", "Places", "Available_bikes", "Capacity", "Status"]
    if not record.isEmpty():
        df = spark.createDataFrame(record, columns)
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
            'es.resource', '%s/%s' % ('velib', 'count'),
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
                                        x[1].get('available_bikes'), x[1].get('bike_stands'), x[1].get('status')))

pairs_registered.foreachRDD(lambda rdd: process_stream(rdd, spark))
# new_rdd = pairs_registered.map(json.dumps).map(lambda x: ('key', x))
# new_rdd.pprint()
# new_rdd.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
#     path='-',
#     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable",
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#     conf={
#         "es.nodes" : 'localhost',
#         "es.port" : '9200',
#         "es.resource" : '%s/%s' % ('index_name', 'doc_type_name'),
#         "es.input.json": 'true'
#     }
# ))

ssc.start()
ssc.awaitTermination()
