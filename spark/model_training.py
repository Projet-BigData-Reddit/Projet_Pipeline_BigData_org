from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,DoubleType
from pyspark.sql.functions import unix_timestamp, to_date,col, regexp_replace,trim,lower
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, Word2Vec, StringIndexer
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second, dayofweek, dayofyear
from transformers import pipeline
from pyspark.sql.functions import pandas_udf
from pyspark.ml.clustering import LDA
import pandas as pd



spark = SparkSession \
    .builder \
    .appName("Local Model training") \
    .master("spark://localhost:7077") \
    .getOrCreate()

schema = StructType([
    StructField("id",StringType()),
    StructField("author",StringType()),
    StructField("subreddit",StringType()),
    StructField("text",StringType()),
    StructField("timestamp",DoubleType()),
    StructField("score",IntegerType()),
    StructField("num_replies",IntegerType())
])

df = spark.read.schema(schema).option("multiLine", "true").json("/opt/spark/work-dir/reddit_crypto_data.json")
df = df.withColumn("timestamp",col("timestamp").cast("timestamp"))

df.printSchema()

# drop nulls
df_clean = df.dropna(subset=["text"])
# remove URLs (http and www)
df_clean = df_clean.withColumn("text",regexp_replace(col("text"),r"https?://\S+", "")) \
  .withColumn("text",regexp_replace(col("text"),r"www\.\S+",""))


# lowercase the text
df_clean = df_clean.withColumn("text",lower(col("text")))

# Remove special characters (keep letters, numbers, spaces)
# regexep_replace(source string column, regular expre pattern to match, string to replace with)
df_clean = df_clean.withColumn("text",regexp_replace(col("text"),r"[^A-Za-z0-9\s]",""))

# trim leading/trailing spaces
df_clean = df_clean.withColumn("text",trim(col("text")))

# replace multiple spaces with a single space
df_clean = df_clean.withColumn("text",regexp_replace(col("text"),r"\s+"," "))

# tokenization
tokenizer = Tokenizer(inputCol="text", outputCol="words")
tokenized_df = tokenizer.transform(df_clean)

# remove stop_words (words that aren't that much meagninful)
stopwords = [
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
remover = StopWordsRemover(inputCol="words",outputCol="filtered_words",stopWords=stopwords)
tokenized_df = remover.transform(tokenized_df)
tokenized_df = tokenized_df.drop("words")



df = tokenized_df.withColumn("year", year("timestamp")) \
                  .withColumn("month", month("timestamp")) \
                  .withColumn("day", dayofmonth("timestamp")) \
                  .withColumn("hour", hour("timestamp")) \
                  .withColumn("minute", minute("timestamp")) \
                  .withColumn("second", second("timestamp")) \
                  .withColumn("day_of_week", dayofweek("timestamp")) \
                  .withColumn("day_of_year", dayofyear("timestamp"))
                  


classifier = pipeline(
    "text-classification", 
    model="AdityaAI9/distilbert_finance_sentiment_analysis"
    )

@pandas_udf(StringType())
def get_sentiment_udf(s:pd.Series) -> pd.Series:
    return pd.Series([result['label'] for result in classifier(s.tolist())])

""""" hadshi khas yrunni f service bohdo
df_with_sentiment = df.withColumn(
    "sentiment",
    get_sentiment_udf(col("text"))
)

df_with_sentiment = df_with_sentiment.drop("text")
"""
word2vec = Word2Vec(
    vectorSize=100,                 # embedding size
    minCount=2,                     # ignore rare words
    inputCol="filtered_words",
    outputCol="word2vec_features",
    windowSize=5,                   # context window
    maxIter=20,                     # training iterations
    stepSize=0.025,                 # learning rate
    seed=42
)
w2v_model = word2vec.fit(df_with_sentiment)

w2v_model.save("/opt/spark/models/word2vec")


cv = CountVectorizer(
    inputCol="filtered_words",
    outputCol="features_lda",
    vocabSize=2000, 
    minDF=3 
)
cv_model = cv.fit(df_with_sentiment)
df_with_cv = cv_model.transform(df_with_sentiment)


# If your LDA wants counts, you may need CountVectorizer; here we assume embeddings
lda = LDA(
    k=6, 
    maxIter=20,
    featuresCol="features_lda", 
    seed=42,
    topicDistributionCol="topic_distribution"
)

# You must train LDA on the DataFrame that contains the CountVectorizer's output
lda_model = lda.fit(df_with_cv) 

# Save model
lda_model.save("/opt/spark/models/lda_model")
cv_model.save("/opt/spark/models/count_vectorizer_model")

# ---------------------------------------------------------
# 5. StringIndexer for Categorical Features (Subreddit, Sentiment)
# ---------------------------------------------------------


# Indexer for Subreddit
subreddit_indexer = StringIndexer(
    inputCol="subreddit", 
    outputCol="subreddit_index"
)
subreddit_model = subreddit_indexer.fit(df_with_sentiment)
df_indexed = subreddit_model.transform(df_with_sentiment)
subreddit_model.save("/opt/spark/models/subreddit_indexer_model")

# Indexer for Sentiment
sentiment_indexer = StringIndexer(
    inputCol="sentiment", 
    outputCol="sentiment_index"
)
sentiment_model = sentiment_indexer.fit(df_indexed)
df_indexed = sentiment_model.transform(df_indexed)
sentiment_model.save("/opt/spark/models/sentiment_indexer_model")

# ---------------------------------------------------------
# 6. VectorAssembler (Combine All Features)
# ---------------------------------------------------------
from pyspark.ml.feature import VectorAssembler

# List all numerical/vector features to be combined
feature_columns = [
    "word2vec_features",
    "topic_distribution",
    "year", "month", "day", "hour",  
    "day_of_week", "day_of_year",
    "subreddit_index",
    "sentiment_index"
]

# Note: LDA and Word2Vec models were fitted on the whole data, 
# but the resulting features are transformed onto the DataFrame here.
df_features = w2v_model.transform(df_indexed)
df_features = lda_model.transform(df_features)

assembler = VectorAssembler(
    inputCols=feature_columns, 
    outputCol="features_regression" # The single column expected by the regressor
)

df_final_features = assembler.transform(df_features)
# VectorAssembler doesn't need to be saved as a model, but the assembler definition can be saved if part of a larger Pipeline.

# ---------------------------------------------------------
# 7. RandomForestRegressor Training
# ---------------------------------------------------------
from pyspark.ml.regression import RandomForestRegressor

# Define the model, using the assembled vector as input
rf = RandomForestRegressor(
    featuresCol="features_regression", 
    labelCol="score", 
    numTrees=30,      # Number of trees (can be tuned for accuracy/speed trade-off)
    maxDepth=10,      # Max depth of each tree (limits complexity)
    seed=42
)

# Train the model on your entire dataset
rf_model = rf.fit(df_final_features)

# Save the trained regression model for real-time inference
rf_model.save("/opt/spark/models/random_forest_model")