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
from config import CLIENT_ID,CLIENT_SECRET, KEYWORDS, SUBREDDITS,USER_AGENT,USERNAME,KAFKA_TOPIC,KAFKA_BROKER_URL,PARTITIONS,REPLICATION_FACTOR
from utils import contains_keywords

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
        except KeyboardInterrupt:
            print("🛑 Stream interrompu manuellement (Ctrl+C).")
        except Exception as e:
            print(f"⚠️ Erreur stream Reddit : {e}")
            time.sleep(5)

if __name__ == "__main__":

    ingestion = DataIngestion(
        CLIENT_ID, CLIENT_SECRET, USER_AGENT,
        KAFKA_TOPIC, KAFKA_BROKER_URL
    )

    ingestion.stream_reddit_comments(SUBREDDITS, KEYWORDS)