# Base Spark image (official)
FROM apache/spark:3.5.1

USER root

# Install curl
RUN apt-get update && apt-get install -y curl && \
    mkdir -p /opt/spark/jars

# Download Spark-Kafka dependencies
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
 && curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
 && curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
 && curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Install Python packages without cache
RUN pip install --no-cache-dir pandas 

WORKDIR /opt/spark/work-dir

USER spark
