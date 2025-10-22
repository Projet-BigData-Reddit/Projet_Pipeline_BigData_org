import websocket
import json
import time

def on_message(ws, message):
    # Convertir le message en dictionnaire
    data = json.loads(message)
    
    # Afficher le contenu de manière lisible
    print("Nouvelle donnée reçue :")
    print(data.get("data"))  # utilisation de get pour éviter les erreurs
    print("-" * 50)  # séparateur pour mieux visualiser
    
    # Ralentir l'affichage (ex: 2 secondes)
    time.sleep(2)

def on_error(ws, error):
    print("Erreur :", error)

def on_close(ws, close_status_code, close_msg):
    print("Connexion fermée")

# Initialisation de la connexion WebSocket
ws = websocket.WebSocketApp(
    "wss://certstream.calidog.io/",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

# Lancement de la boucle WebSocket
ws.run_forever()
