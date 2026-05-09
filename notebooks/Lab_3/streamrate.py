## uruchom przez spark-submit streamrate.py
## spakr-submit - skypty Pythonowe w Sparku
## Skrypt przetwarzania strumieniowego


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
)

# Ukryta transofrmacja df = 1*df
query = (df.writeStream 
    .format("console")          # Efekty w terminalu
    .outputMode("append")       # Domyślnie też na append
    .option("truncate", False)  # Wyświetlanie w całości
    .start()
) 

query.awaitTermination()    # Jeżeli kod jako sktrypt to ten kod zapewnia poprawne zakończenie działania
