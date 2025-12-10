# engine.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, when, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler

# Imports locaux
import config
from loader import ModelLoader
from preprocessor import DataPreprocessor

class RedditInferenceEngine:
    def __init__(self):
        # 1. Init Spark
        self.spark = SparkSession.builder \
            .appName(config.APP_NAME) \
            .master(config.MASTER) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # 2. Chargement des modèles
        print("📥 Chargement des modèles ML...")
        self.loader = ModelLoader(config.MODEL_PATHS)
        self.models = self.loader.load_all()
        
        # 3. --- GÉNÉRATION DES LABELS DE SUJETS ---
        # On traduit les IDs mathématiques en mots compréhensibles
        print("🧠 Décodage des sujets LDA...")
        self.topic_labels = self._generate_topic_labels()
        print(f"✅ Sujets identifiés : {self.topic_labels}")
        
        # 4. Init Preprocessor
        self.preprocessor = DataPreprocessor()
        print("✅ Moteur Spark prêt.")

    def _generate_topic_labels(self):
        """
        Croise le modèle LDA et le CountVectorizer pour donner un nom aux sujets.
        """
        try:
            # 1. On récupère le vocabulaire (la liste des mots : ['bitcoin', 'crypto', ...])
            vocab = self.models['cv'].vocabulary
            
            # 2. On demande au LDA les 3 mots les plus importants par sujet
            # Cela retourne un DataFrame : [topic, termIndices, termWeights]
            topics_df = self.models['lda'].describeTopics(maxTermsPerTopic=3)
            
            # 3. On collecte les résultats sur le driver pour créer un dictionnaire Python
            rows = topics_df.collect()
            
            topic_map = {}
            for row in rows:
                topic_id = row['topic']
                indices = row['termIndices']
                # On remplace les indices (ex: 45) par le mot (ex: "wallet")
                words = [vocab[i] for i in indices]
                # On crée une chaîne : "wallet-key-lost"
                topic_map[topic_id] = "-".join(words)
                
            return topic_map
        except Exception as e:
            print(f"⚠️ Impossible de nommer les sujets : {e}")
            return {}

    def _transform_batch(self, batch_df):
        """Pipeline de transformation."""
        df = self.preprocessor.clean_text(batch_df)
        df = self.preprocessor.tokenize_and_remove_stopwords(df)
        df = self.preprocessor.extract_time_features(df)
        
        # Sentiment
        df = df.withColumn("sentiment", DataPreprocessor.get_sentiment_udf(col("text")))
        
        # NLP Models
        df = self.models['w2v'].transform(df)
        df = self.models['cv'].transform(df)
        df = self.models['lda'].transform(df) # Crée 'topic_distribution'
        
        df = self.models['sub'].transform(df)
        df = self.models['sent'].transform(df)
        
        # Assemblage
        assembler = VectorAssembler(
            inputCols=[
                "word2vec_features", "topic_distribution",
                "year", "month", "day", "hour", "day_of_week", "day_of_year",
                "subreddit_index", "sentiment_index"
            ],
            outputCol="features_regression",
            handleInvalid="skip"
        )
        return assembler.transform(df)

    def process_and_predict(self, batch_df, batch_id):
        if batch_df.isEmpty(): return

        print(f"⚙️ Traitement du Batch {batch_id}...")
        
        try:
            final_df = self._transform_batch(batch_df)
            predictions = self.models['rf'].transform(final_df)
            
            # --- UDF INTELLIGENTE POUR LE SUJET ---
            # Cette fonction va chercher le nom du sujet dans le dictionnaire self.topic_labels
            topic_map = self.topic_labels # On copie la ref pour l'UDF
            
            def get_topic_name(distribution_vector):
                # 1. Trouver l'index avec la plus grande probabilité (argmax)
                idx = int(distribution_vector.argmax())
                # 2. Retourner les mots correspondants
                return topic_map.get(idx, f"Sujet {idx}")

            # Enregistrement de l'UDF
            topic_udf = udf(get_topic_name, StringType())
            
            display_df = predictions.withColumn("sujet_mots", topic_udf(col("topic_distribution")))
            
            # Mise en forme
            display_df = display_df.withColumn("score_predit", round(col("prediction"), 2))
            display_df = display_df.withColumn("viralite", 
                                               when(col("prediction") > 5, "🔥 HOT")
                                               .when(col("prediction") > 2, "📈 Up")
                                               .otherwise("zzz Low"))

            print("📊 RÉSULTATS DÉTAILLÉS :")
            # Affichage avec les mots clés du sujet
            display_df.select("subreddit", "sentiment", "sujet_mots", "score_predit", "viralite", "text") \
                      .show(10, truncate=30)
            
        except Exception as e:
            print(f"⚠️ Erreur Batch {batch_id}: {e}")

    def run(self):
        print(f"🎧 Écoute de {config.KAFKA_TOPIC}...")
        
        schema = StructType([
            StructField("id", StringType()),
            StructField("author", StringType()),
            StructField("subreddit", StringType()),
            StructField("text", StringType()),
            StructField("timestamp", DoubleType()),
            StructField("score", IntegerType())
        ])

        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BROKER_URL) \
            .option("subscribe", config.KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        json_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        query = json_stream.writeStream \
            .foreachBatch(self.process_and_predict) \
            .trigger(processingTime="5 seconds") \
            .start()

        query.awaitTermination()