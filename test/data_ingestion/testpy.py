import praw
import time
import json
import re
import os
from config import CLIENT_ID, CLIENT_SECRET, USER_AGENT

# ---------- Connexion à Reddit ----------
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

# ---------- Subreddits et mots-clés ----------
subreddits = "cryptocurrency+Bitcoin+Ethereum+altcoin+CryptoMarkets+ethtrader+CryptoTechnology+CryptoCurrencyNews+DeFi+CryptoMoonShots+Dogecoin+Cardano+Solana+ShibaInu"
keywords = [
    "crypto", "cryptocurrency", "bitcoin", "btc", "ethereum", "eth",
    "blockchain", "altcoin", "token", "defi", "nft", "smart contract",
    "mining", "miner", "hash rate", "wallet", "hardware wallet", "cold storage",
    "staking", "yield farming", "airdrop", "ico", "ido", "web3",
    "dogecoin", "doge", "cardano", "ada", "solana", "sol", "shiba inu", "shib"
]
keywords = [k.lower() for k in keywords]

# ---------- Fichier de sortie ----------

output_file = "reddit_crypto_data.json"
if not os.path.exists(output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump([], f)

# ---------- Fonctions utilitaires ----------
def is_relevant(text):
    """Vérifie si le commentaire contient un mot-clé pertinent"""
    if not text:
        return False
    text = text.lower()
    return any(re.search(r'\b{}\b'.format(re.escape(k)), text) for k in keywords)


def save_comment(data):
    """Sauvegarde un commentaire unique dans le fichier JSON"""
    try:
        # créer fichier vide si absent
        if not os.path.exists(output_file):
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump([], f)

        with open(output_file, "r+", encoding="utf-8") as f:
            try:
                comments = json.load(f)
            except json.JSONDecodeError:
                comments = []  # si le JSON est corrompu

            # éviter les doublons via l'ID du commentaire
            if not any(c["id"] == data["id"] for c in comments):
                comments.append(data)

            # réécriture propre du fichier
            f.seek(0)
            json.dump(comments, f, ensure_ascii=False, indent=2)
            f.truncate()

    except Exception as e:
        print("Erreur sauvegarde:", e)


# ---------- Récupérer anciens commentaires ----------
def fetch_old_comments(limit=1000):
    print("📥 Récupération des anciens commentaires...")
    try:
        subreddit = reddit.subreddit(subreddits)
        for comment in subreddit.comments(limit=limit):
            if is_relevant(comment.body):
                data = {
                    "id": comment.id,
                    "author": str(comment.author),
                    "subreddit": str(comment.subreddit),
                    "text": comment.body,
                    "timestamp": comment.created_utc,
                    "score": comment.score,
                    "num_replies": len(comment.replies)
                }
                print(f"[Ancien] {data['text']}")
                save_comment(data)
        print("✅ Récupération des anciens commentaires terminée.")
    except Exception as e:
        print("⚠️ Erreur récupération anciens commentaires:", e)


# ---------- Stream en temps réel ----------
def stream_new_comments():
    print("📡 Écoute des nouveaux commentaires en temps réel...")
    try:
        for comment in reddit.subreddit(subreddits).stream.comments(skip_existing=True):
            try:
                if is_relevant(comment.body):
                    data = {
                        "id": comment.id,
                        "author": str(comment.author),
                        "subreddit": str(comment.subreddit),
                        "text": comment.body,
                        "timestamp": comment.created_utc,
                        "score": comment.score,
                        "num_replies": len(comment.replies)
                    }

                    print(f"[Nouveau] {data['text']}")
                    save_comment(data)
            except Exception as e:
                print("Erreur traitement commentaire:", e)
                time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Arrêt du stream Reddit (Ctrl+C).")
    except Exception as e:
        print("⚠️ Erreur générale stream:", e)
        time.sleep(5)


# ---------- Main ----------
if __name__ == "__main__":
    fetch_old_comments(limit=500)   # Récupère les 500 derniers commentaires pertinents
    stream_new_comments()           # Puis écoute en temps réel
