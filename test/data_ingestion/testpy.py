import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config import KAFKA_TOPIC
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# --- Paramètres de Connexion ---
# C'est l'adresse que vous avez exposée avec Docker
KAFKA_BROKER_URL = 'localhost:9092'

# --- Paramètres du Topic ---
PARTITIONS = 3
REPLICATION_FACTOR = 1 # Doit être 1 car nous n'avons qu'un seul broker

def create_kafka_topic():
    """
    Tente de se connecter à Kafka et de créer un topic.
    Gère les erreurs de connexion et le cas où le topic existe déjà.
    """
    print(f"Tentative de connexion au broker Kafka à l'adresse : {KAFKA_BROKER_URL}")

    try:
        # 1. Créer un client d'administration.
        #    Cette ligne est le premier test de connexion.
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER_URL,
            client_id='kafka_test_client'
        )
        print("✅ Connexion au broker Kafka réussie !")

        # 2. Définir le nouveau topic
        topic = NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        print(f"Tentative de création du topic '{KAFKA_TOPIC}'...")

        # 3. Essayer de créer le topic
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"🎉 Topic '{KAFKA_TOPIC}' créé avec succès !")

    except NoBrokersAvailable:
        print(f"❌ ERREUR DE CONNEXION : Impossible de se connecter au broker Kafka.")
        print("   Veuillez vérifier les points suivants :")
        print("   1. Le conteneur Docker Kafka est-il bien en cours d'exécution ? (Vérifiez avec 'docker ps')")
        print("   2. L'adresse du broker est-elle correcte ? (devrait être 'localhost:9092')")
        
    except TopicAlreadyExistsError:
        print(f"👍 Le topic '{KAFKA_TOPIC}' existe déjà. Tout est en ordre.")
        
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")
        
    finally:
        if 'admin_client' in locals():
            admin_client.close()
            print("Connexion admin fermée.")

# --- Lancement du script ---
if __name__ == "__main__":
    create_kafka_topic()