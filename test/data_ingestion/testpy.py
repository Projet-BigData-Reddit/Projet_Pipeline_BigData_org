# Fichier: reddit_to_kafka_pipeline.py

import praw
import time
import json
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# =============================================================================
# ---                      PARTIE 1: CONFIGURATION                          ---
# =============================================================================

# --- Paramètres Kafka ---
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'reddit-comments'
PARTITIONS = 3
REPLICATION_FACTOR = 1  # Doit être 1 car nous n'avons qu'un seul broker

# --- Paramètres Reddit (remplacez par vos propres identifiants) ---
# Vous devez obtenir ces informations en créant une application sur Reddit
CLIENT_ID = "VOTRE_CLIENT_ID"
CLIENT_SECRET = "VOTRE_CLIENT_SECRET"
USER_AGENT = "Pipeline de données v1.0 par u/VOTRE_NOM_UTILISATEUR"

# --- Paramètres de recherche pour la Coupe du Monde Féminine 2025 ---
SUBREDDITS_A_ECOUTER = "WomensSoccer+USWNT+NWSL+soccer+fussball"
MOTS_CLES = [
    "usa", "uswnt", "germany", "deutschland", "allemagne", "dfb frauen", 
    "goal", "tor", "penalty", "finale", "world cup"
]

# =============================================================================
# ---                   PARTIE 2: FONCTIONS UTILITAIRES                     ---
# =============================================================================

def create_kafka_topic():
    """
    Se connecte à Kafka et s'assure que le topic existe.
    C'est la première étape avant de lancer le producteur.
    """
    print("--- Étape 1: Vérification du topic Kafka ---")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER_URL,
            client_id='topic_setup_client'
        )
        print("✅ Connexion admin à Kafka réussie !")

        topic = NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"🎉 Topic '{KAFKA_TOPIC}' créé avec succès !")
        admin_client.close()

    except TopicAlreadyExistsError:
        print(f"👍 Le topic '{KAFKA_TOPIC}' existe déjà. Aucune action n'est nécessaire.")
        admin_client.close()
    except NoBrokersAvailable:
        print(f"❌ ERREUR : Impossible de se connecter au broker Kafka à l'adresse {KAFKA_BROKER_URL}.")
        print("   Veuillez vous assurer que le conteneur Docker Kafka est bien en cours d'exécution.")
        return False
    except Exception as e:
        print(f"Une erreur inattendue est survenue lors de la création du topic : {e}")
        return False
    
    return True

def create_kafka_producer():
    """
    Crée et retourne un producteur Kafka, prêt à envoyer des messages.
    """
    print("\n--- Étape 2: Démarrage du producteur Kafka ---")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            # Sérialise les messages en format JSON puis les encode en bytes
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Producteur Kafka connecté et prêt !")
        return producer
    except NoBrokersAvailable:
        print("❌ ERREUR : Impossible de créer le producteur. Broker non disponible.")
        return None
    except Exception as e:
        print(f"Une erreur inattendue est survenue lors de la création du producteur : {e}")
        return None

def stream_reddit_comments(producer):
    """
    Se connecte à Reddit, écoute les nouveaux commentaires en temps réel
    et les envoie à Kafka via le producteur fourni.
    """
    print("\n--- Étape 3: Lancement du streaming Reddit ---")
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT
        )
        print(f"✅ Connecté à Reddit en tant que {reddit.user.me()}")
        print(f"📡 Écoute des nouveaux commentaires sur : r/{SUBREDDITS_A_ECOUTER.replace('+', ', r/')}")

        for comment in reddit.subreddit(SUBREDDITS_A_ECOUTER).stream.comments(skip_existing=True):
            # Vérifie si le commentaire contient un mot-clé pertinent
            if any(keyword in comment.body.lower() for keyword in MOTS_CLES):
                data = {
                    "id": comment.id,
                    "author": str(comment.author),
                    "subreddit": str(comment.subreddit),
                    "text": comment.body,
                    "timestamp_utc": comment.created_utc,
                }
                
                print(f"💬 [Commentaire pertinent trouvé] -> Envoi à Kafka...")
                # Envoi des données au topic Kafka
                producer.send(KAFKA_TOPIC, value=data)
                
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du streaming demandé par l'utilisateur (Ctrl+C).")
    except Exception as e:
        print(f"⚠️ Une erreur critique est survenue dans le stream Reddit : {e}")

# =============================================================================
# ---                       PARTIE 3: POINT D'ENTRÉE                        ---
# =============================================================================

if __name__ == "__main__":
    # Étape 1: S'assurer que le topic existe. Si ça échoue, on arrête tout.
    if create_kafka_topic():
        
        # Étape 2: Créer le producteur Kafka.
        kafka_producer = create_kafka_producer()
        
        # Étape 3: Si le producteur est bien créé, on lance le stream.
        if kafka_producer:
            stream_reddit_comments(kafka_producer)
            
            # Nettoyage à la fin du script
            print("Fermeture du producteur Kafka...")
            kafka_producer.close()
            print("Producteur fermé. Au revoir !")
        else:
            print("Le script ne peut pas continuer car le producteur Kafka n'a pas pu être créé.")