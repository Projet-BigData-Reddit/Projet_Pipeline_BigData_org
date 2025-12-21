from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, when, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
import pymongo

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
            .config("spark.cassandra.connection.host", config.CASSANDRA) \
            .config("spark.cassandra.connection.port", config.CASSANDRA_PORT) \
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
            # rows est une liste de row, chaque row est un topic LDA qui contient : topic, termIndeces et termWeights.
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
    def save_to_mongo(self, df_spark):
            """Fonction de secours : Sauvegarde dans MongoDB si Cassandra échoue"""
            try:
                # Conversion Spark -> Pandas -> Dictionnaire (Nécessaire pour PyMongo)
                records = df_spark.toPandas().to_dict(orient='records')
                
                if not records:
                    return

                # Création de l'URI de connexion avec authentification
                uri = f"mongodb://{config.MONGO_USER}:{config.MONGO_PASS}@{config.MONGO_HOST}:{config.MONGO_PORT}/?authSource=admin"
                
                # Connexion et insertion
                client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
                db = client[config.MONGO_DB]
                collection = db[config.MONGO_COLLECTION]
                
                collection.insert_many(records)
                print(f"✅ SAUVEGARDE MONGODB RÉUSSIE : {len(records)} posts sauvés (Fallback).")
                client.close()
                
            except Exception as e:
                print(f"❌ ÉCHEC CRITIQUE : Impossible de sauver dans MongoDB non plus. Erreur : {e}")

    def process_and_save(self, batch_df, batch_id):
            if batch_df.isEmpty(): return

            print(f"⚙️ Traitement Batch {batch_id}...")
            
            try:
                # A. Prédictions & Enrichissement
                final_df = self._transform_batch(batch_df)
                predictions = self.models['rf'].transform(final_df)
                
                topic_map = self.topic_labels
                def get_topic_name(v):
                    return topic_map.get(int(v.argmax()), "Autre")
                topic_udf = udf(get_topic_name, StringType())
                
                enrichi_df = predictions.withColumn("sujet_mots", topic_udf(col("topic_distribution"))) \
                                        .withColumn("score_predit", round(col("prediction"), 2)) \
                                        .withColumn("viralite", 
                                                when(col("prediction") > 3.0, "HOT")
                                                .when(col("prediction") > 1.5, "UP")
                                                .otherwise("LOW"))

                # Préparation du DataFrame final pour le stockage
                storage_df = enrichi_df.select(
                    col("id"), col("author"), col("subreddit"),
                    col("text").alias("text_content"),
                    col("sentiment"),
                    col("sujet_mots").alias("sujet"),
                    col("score_predit").cast("float"),
                    col("viralite"),
                    to_timestamp(col("timestamp")).alias("creation_date")
                )

                # --- LE BLOC DE SÉCURITÉ (TRY CASSANDRA -> EXCEPT MONGO) ---
                try:
                    print(f"💾 Tentative sauvegarde Cassandra ({storage_df.count()} items)...")
                    storage_df.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .options(table="viral_posts", keyspace="reddit_db") \
                        .mode("append") \
                        .save()
                    print("✅ Sauvegarde Cassandra réussie !")
                
                except Exception as e_cassandra:
                    print(f"⚠️ ERREUR CASSANDRA : {e_cassandra}")
                    print("🔄 Basculement vers MongoDB (Fallback)...")
                    self.save_to_mongo(storage_df)
                # -----------------------------------------------------------

            except Exception as e:
                print(f"⚠️ Erreur Générale Batch {batch_id}: {e}")

    def run(self):
            print(f"🎧 Écoute de {config.KAFKA_TOPIC} avec Checkpointing...")
            
            schema = StructType([
                StructField("id", StringType()),
                StructField("author", StringType()),
                StructField("subreddit", StringType()),
                StructField("text", StringType()),
                StructField("timestamp", DoubleType()),
                StructField("score", IntegerType())
            ])

            # Lecture Kafka (EARLIEST pour ne rien rater au premier lancement)
            raw_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", config.KAFKA_BROKER_URL) \
                .option("subscribe", config.KAFKA_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("maxOffsetsPerTrigger", 50) \
                .option("failOnDataLoss", "false") \
                .load()
            # le champs value est du bytes, CAST(value as STRING) le change depuis bytes en string
            # from_json(.., schema) : une colonne unique (data par exemple) qui contient un objet avec des champs id et text. [ouvre le JSON et structure les champs]
            # select("data.*") → crée des colonnes pour chaque champ du message
            json_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("data")) \
                .select("data.*")

            # Chemin persistant pour le marque-page
            checkpoint_dir = "/opt/spark/work-dir/checkpoints"

            query = json_stream.writeStream \
                .foreachBatch(self.process_and_save) \
                .trigger(processingTime="20 seconds") \
                .option("checkpointLocation", checkpoint_dir) \
                .start()

            query.awaitTermination()