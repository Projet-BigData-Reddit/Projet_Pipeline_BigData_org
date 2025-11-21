from pyspark.sql import SparkSession
from config import APP_NAME, MASTER, KAFKA_BROKER_URL, KAFKA_TOPIC

class SparkTransformation:
    """
    Classe pour encapsuler un job Spark ETL avec Kafka comme source.
    """
    def __init__(self, app_name):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master(MASTER) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.5.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN") # Pour réduire le bruit dans les logs
        print("✅ Spark session initialized with Kafka support.")

    def consume_and_display(self):
        print(f"🎧 Listening to Kafka topic: {KAFKA_TOPIC} at {KAFKA_BROKER_URL}...")

        # 1. Lecture du flux Kafka
        df_kafka = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

        # 2. Conversion des données binaires en String pour l'affichage
        # Kafka stocke les données dans la colonne 'value' sous forme binaire
        df_readable = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # 3. Affichage dans la console (Trigger processing)
        query = df_readable.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        query.awaitTermination()

# --- Pour lancer le test ---
if __name__ == "__main__":
    etl_job = SparkTransformation(APP_NAME)
    etl_job.consume_and_display()