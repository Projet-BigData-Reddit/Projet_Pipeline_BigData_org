import requests
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, trim, lower, year, month, dayofmonth, hour, dayofweek, dayofyear, pandas_udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import config 

class DataPreprocessor:
    """
    Classe responsable du nettoyage, tokenization, feature engineering et appel API.
    """
    
    def clean_text(self, df: DataFrame) -> DataFrame:
        """Nettoyage Regex et formatage basique."""
        df = df.dropna(subset=["text"])
        df = df.withColumn("text", lower(col("text")))
        # Suppression URLs et caractères spéciaux
        df = df.withColumn("text", regexp_replace(col("text"), r"https?://\S+|www\.\S+|[^A-Za-z0-9\s]", ""))
        # Suppression espaces multiples
        df = df.withColumn("text", trim(regexp_replace(col("text"), r"\s+", " ")))
        return df

    def extract_time_features(self, df: DataFrame) -> DataFrame:
        """Extraction des features temporelles."""
        return df.withColumn("datetime", col("timestamp").cast("timestamp")) \
                 .withColumn("year", year("datetime")) \
                 .withColumn("month", month("datetime")) \
                 .withColumn("day", dayofmonth("datetime")) \
                 .withColumn("hour", hour("datetime")) \
                 .withColumn("day_of_week", dayofweek("datetime")) \
                 .withColumn("day_of_year", dayofyear("datetime"))

    def tokenize_and_remove_stopwords(self, df: DataFrame) -> DataFrame:
        """Tokenization NLP."""
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        df = tokenizer.transform(df)
        
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=config.STOPWORDS)
        return remover.transform(df)

    # Note: Cette méthode est statique car Spark la sérialise différemment
    @staticmethod
    @pandas_udf(StringType())
    def get_sentiment_udf(series: pd.Series) -> pd.Series:
        """Appel API optimisé (Batch)."""
        texts = series.tolist()
        if not texts: return pd.Series([], dtype="string")
        try:
            response = requests.post(config.API_URL, json={"texts": texts}, timeout=5)
            if response.status_code == 200:
                return pd.Series(response.json()["labels"])
        except Exception as e:
            # En prod, on pourrait logger l'erreur ici
            pass
        return pd.Series(["neutral"] * len(texts))