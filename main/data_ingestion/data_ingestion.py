import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
import praw
import time
import json
import re
from typing import List  
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import  NoBrokersAvailable
from config import CLIENT_ID,CLIENT_SECRET, KEYWORDS, SUBREDDITS,USER_AGENT,USERNAME,KAFKA_TOPIC,KAFKA_BROKER_URL,PARTITIONS,REPLICATION_FACTOR,AZURE_CONTAINER_NAME,AZURE_CONNECTION_STRING
from utils import contains_keywords
from azure.storage.filedatalake import DataLakeServiceClient

class DataIngestion:
    """
    Classe responsable de l'ingestion de données Reddit et de leur envoi vers Kafka (streaming temps réel).
    """

    def __init__(self, client_id, client_secret, user_agent,
                 kafka_topic, kafka_broker_url,
                 partitions=1, replication_factor=1):

        self.kafka_topic = kafka_topic
        self.kafka_broker_url = kafka_broker_url
        self.partitions = partitions
        self.replication_factor = replication_factor

        # 🔹 Connexion Reddit
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        self.create_kafka_topic()
        self.producer = self.create_kafka_producer()

        print("✅ Initialisation complète : Reddit → Kafka prête.")
        # 🔹 NOUVEAU : Initialisation Azure
        self.connection_string = AZURE_CONNECTION_STRING # Stockage nécessaire
        self.azure_service_client = DataLakeServiceClient.from_connection_string(self.connection_string)
        self.container_name = AZURE_CONTAINER_NAME
        self.buffer = []  
        self.buffer_limit = 10


    # ==============================================================
    # MÉTHODES KAFKA
    # ==============================================================

    def create_kafka_topic(self):
        """Crée le topic Kafka s'il n'existe pas déjà."""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_broker_url,
                client_id='topic_setup_client'
            )

            existing_topics = admin_client.list_topics()

            if self.kafka_topic in existing_topics:
                print(f"👍 Le topic '{self.kafka_topic}' existe déjà, aucune action nécessaire.")
            else:
                topic = NewTopic(
                    name=self.kafka_topic,
                    num_partitions=self.partitions,
                    replication_factor=self.replication_factor
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                print(f"🎉 Topic '{self.kafka_topic}' créé avec succès !")

        except NoBrokersAvailable:
            print("❌ Aucun broker Kafka disponible. Vérifie que ton serveur Kafka est en ligne.")
        except Exception as e:
            print(f"⚠️ Erreur lors de la création du topic Kafka : {e}")
        finally:
            try:
                admin_client.close()
            except Exception:
                pass


    def create_kafka_producer(self):
        """Crée un producteur Kafka."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker_url,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Producteur Kafka initialisé.")
            return producer
        except Exception as e:
            print(f"Erreur création producteur Kafka : {e}")
            return None


    def send_to_kafka(self, data):
        """Envoie un message JSON vers Kafka."""
        if not self.producer:
            print("⚠️ Producteur Kafka non initialisé.")
            return
        try:
            self.producer.send(self.kafka_topic, value=data)
            print(data)
            self.producer.flush() 
            print(f"📤 Envoyé sur Kafka : {data['id']} ({data['subreddit']})")
        except Exception as e:
            print(f"Erreur envoi Kafka : {e}")


    # ==============================================================
    # REDDIT STREAMING
    # ==============================================================

    def is_relevant(self, text, keywords):
        """Vérifie si un texte contient au moins un mot-clé pertinent."""
        return contains_keywords(text,keywords)


    def stream_reddit_comments(self, subreddits: str, keywords: List[str]):
        """Stream en temps réel des commentaires Reddit vers Kafka."""
        print(f"📡 Démarrage du streaming Reddit → Kafka sur '{self.kafka_topic}'...")
        try:
            for comment in self.reddit.subreddit(subreddits).stream.comments(skip_existing=True):
                if self.is_relevant(comment.body, keywords):
                    data = {
                        "id": comment.id,
                        "author": str(comment.author),
                        "subreddit": str(comment.subreddit),
                        "text": comment.body,
                        "timestamp": comment.created_utc,
                        "score": comment.score
                    }
                    self.send_to_kafka(data)
                    # 2. MISE EN ATTENTE POUR AZURE
                    self.buffer.append(data)

                    # 3. SI LE SAC EST PLEIN, ON ENVOIE SUR AZURE
                    if len(self.buffer) >= self.buffer_limit:
                        self.save_to_datalake()

        except KeyboardInterrupt:
            if self.buffer: # Sauvegarde ce qui reste avant de couper
                self.save_to_datalake()
            print("🛑 Arrêt du script.")
        except Exception as e:
            print(f"⚠️ Erreur stream Reddit : {e}")
            time.sleep(5)
            
    def save_to_datalake(self):
        """Append the buffer to a single file in Azure Data Lake Gen2"""
        if not self.buffer:
            return

        try:
            # Use the client from __init__
            file_system_client = self.azure_service_client.get_file_system_client(file_system=self.container_name)
            
            # Single target file
            filename = "raw_reddit.json"
            file_client = file_system_client.get_file_client(f"bronze/{filename}")
            
            # Convert buffer to JSON lines (one dict per line)
            # This way we can append without breaking JSON
            data_to_upload = "\n".join(json.dumps(item) for item in self.buffer) + "\n"
            data_bytes = data_to_upload.encode('utf-8')
            
            # If file does not exist, create it
            if not file_client.exists():
                file_client.create_file()
            
            # Append at the end
            file_client.append_data(data_bytes, offset=file_client.get_file_properties().size, length=len(data_bytes)) # Offset tells Azure where to start writing → at the end of the file
            # flush_data(length) → Commit the data you just appended
            file_client.flush_data(file_client.get_file_properties().size + len(data_bytes))
            
            print(f"📦 Appended {len(self.buffer)} messages to {filename} on Azure.")
            
            # Clear the buffer
            self.buffer = []
        except Exception as e:
            print(f"❌ Error saving to Azure: {e}")



if __name__ == "__main__":

    ingestion = DataIngestion(
        CLIENT_ID, CLIENT_SECRET, USER_AGENT,
        KAFKA_TOPIC, KAFKA_BROKER_URL
    )

    ingestion.stream_reddit_comments(SUBREDDITS, KEYWORDS)