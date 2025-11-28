from dotenv import load_dotenv
import os
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

KAFKA_TOPIC = "Reddit_Data"
KAFKA_BROKER_URL = 'localhost:9093'
PARTITIONS = 1
REPLICATION_FACTOR = 1

SUBREDDITS = "cryptocurrency+Bitcoin+Ethereum+altcoin+CryptoMarkets+ethtrader+CryptoTechnology+CryptoCurrencyNews+DeFi+CryptoMoonShots+Dogecoin+Cardano+Solana+ShibaInu"
KEYWORDS = [
    "crypto", "cryptocurrency", "bitcoin", "btc", "ethereum", "eth",
    "blockchain", "altcoin", "token", "defi", "nft", "smart contract",
    "mining", "miner", "hash rate", "wallet", "hardware wallet", "cold storage",
    "staking", "yield farming", "airdrop", "ico", "ido", "web3",
    "dogecoin", "doge", "cardano", "ada", "solana", "sol", "shiba inu", "shib"
]

