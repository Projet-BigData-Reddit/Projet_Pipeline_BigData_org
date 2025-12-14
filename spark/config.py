KAFKA_TOPIC = "Reddit_Data"
KAFKA_BROKER_URL = "broker:9092"
PARTITIONS = 1
REPLICATION_FACTOR = 1
APP_NAME = "RedditETLTraitement"
MASTER = "spark://spark-master:7077"

MODEL_PATHS = {
    "word2vec": "/opt/spark/models/word2vec",
    "cv": "/opt/spark/models/count_vectorizer_model",
    "lda": "/opt/spark/models/lda_model",
    "subreddit": "/opt/spark/models/subreddit_indexer_model",
    "sentiment": "/opt/spark/models/sentiment_indexer_model",
    "rf": "/opt/spark/models/random_forest_model"
}

API_URL = "http://sentiment-container:8000/predict"

STOPWORDS = [
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by","can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during","each","few", "for", "from", "further",
    "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
    "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself","let's","me", "more", "most", "mustn't", "my", "myself","no", "nor", "not",
    "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own",
    "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too","under", "until", "up","very","was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't",
    "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
]

CASSANDRA = "cassandra"
CASSANDRA_PORT = "9042"