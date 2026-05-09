from pyspark.sql import SparkSession
from pyspark.sql.functions import col
 
spark = (
    SparkSession.builder
    .appName("Lab4-Kafka")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
 
kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .load()
)
 
df = kafka_raw.select("value")
 
query = (df.writeStream 
    .format("console") 
    .outputMode("append")
    #.option("truncate", False) 
    .start()
)
 
query.awaitTermination()
