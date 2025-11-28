from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Fonction pour transformer timestamp UNIX → datetime
def convert_timestamp(df: DataFrame, col_name: str = "timestamp") -> DataFrame:
    """
    Convertit un timestamp UNIX (float) en datetime complet (yyyy-MM-dd HH:mm:ss)
    et ajoute une colonne 'datetime'
    """
    return df.withColumn(
        "datetime",
        F.to_timestamp(F.from_unixtime(F.col(col_name)))
    )

# Fonction mapPartitions pour analyse de sentiment
def sentiment_partition(iterator):
    """
    Structure prête pour appliquer le modèle de sentiment sur chaque partition.
    Pour le moment, la fonction ne charge pas encore de modèle.
    Chaque élément de l'itérateur est un Row avec 'text'.
    """
    # Exemple : juste renvoyer le texte sans changement (placeholder)
    for row in iterator:
        # Ici tu pourras plus tard charger un modèle HuggingFace ou autre
        sentiment = "NEUTRAL"  # placeholder
        yield Row(
            id=row['id'] if 'id' in row else None,
            subreddit=row['subreddit'] if 'subreddit' in row else None,
            text=row['text'] if 'text' in row else None,
            score=row['score'] if 'score' in row else None,
            sentiment=sentiment
        )
