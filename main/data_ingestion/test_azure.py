from azure.storage.filedatalake import DataLakeServiceClient
from config import AZURE_CONNECTION_STRING, AZURE_CONTAINER_NAME

def test_connection():
    try:
        # 1. On se connecte au service Azure
        service_client = DataLakeServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        
        # 2. On cible ton conteneur
        file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER_NAME)
        
        # 3. On crée un fichier de test
        file_client = file_system_client.get_file_client("test_ingenieur.txt")
        
        # 4. On écrit un message dedans
        message = "Bravo ! La connexion entre Python et Azure Data Lake fonctionne."
        file_client.upload_data(message, overwrite=True)
        
        print("✅ SUCCÈS : Le fichier a été créé sur Azure !")
        
    except Exception as e:
        print(f"❌ ÉCHEC : Quelque chose ne va pas : {e}")

if __name__ == "__main__":
    test_connection()