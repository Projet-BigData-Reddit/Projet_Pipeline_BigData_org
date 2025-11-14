# Base image Spark
FROM bitnami/spark:3.4.1

WORKDIR /opt/bitnami/spark

# Passe en root pour installer les paquets
USER root

# Installe curl et crée le dossier pour les JARs
RUN install_packages curl && \
    mkdir -p /opt/bitnami/spark/jars

# --- Télécharge TOUTES les dépendances nécessaires ---

# 2. Connecteur Spark-SQL-Kafka (INCHANGÉ)
RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar

# 3. La dépendance manquante : commons-pool2 (AJOUTÉ)
RUN curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# 4. Client Kafka (VERSION MISE À JOUR)
RUN curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

# 5. Token Provider Spark-Kafka (INCHANGÉ)
RUN curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar
