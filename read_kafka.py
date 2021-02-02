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
import json  # Spark context details

sc = SparkContext(appName="PythonSparkStreamingKafka")
ssc = StreamingContext(sc, 6)  # Creating Kafka direct stream
dks = KafkaUtils.createDirectStream(ssc, ["velib"],
                                    {"metadata.broker.list": "localhost:9092"})


pairs = dks.map(lambda x: (int(x[0]), json.loads(x[1])))

pairs_registered = pairs.map(lambda x: (x[0],
                                        [str(datetime.now().strftime("%m/%d/%Y %H:%M")), x[1].get("last_update"),
                                        x[1].get("number"), x[1].get("available_bike_stands"),
                                        x[1].get('available_bikes'), x[1].get('bike_stands'), x[1].get('status')]))

pairs_registered.pprint()

ssc.start()
ssc.awaitTermination()
