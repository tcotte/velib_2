from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType
from pyspark.sql.functions import udf
from datetime import datetime
from pyspark.sql.functions import to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import lit
from pyspark.sql.functions import window

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

schema = StructType(
    [
        StructField('number', IntegerType(), True),
        StructField('last_update', TimestampType(), True),
        StructField('available_bikes', IntegerType(), True),
    ]
)


# Create DataFrame representing the stream of input lines from connection to localhost:9092
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "velopredict") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \


formated_df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select("parsed_value.*")
# formated_df = formated_df.withColumn("last_update", get_timestamp(formated_df['last_update']))
# formated_df = formated_df.select(to_timestamp(formated_df['last_update'], 'yyyy-MM-dd HH:mm:ss').alias('date'))
formated_df = formated_df.filter(formated_df["number"] < 1000)
# df_with_x4 = formated_df.withColumn("x4", lit(str(datetime.now())))

# formated_df = formated_df.withWatermark("last_update","20 minutes")
# formated_df = formated_df.groupBy(
#     window(formated_df.last_update, "10 minutes", "1 minutes"))

query = formated_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

