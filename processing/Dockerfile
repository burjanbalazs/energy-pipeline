FROM apache/spark:4.0.2-scala2.13-java17-python3-ubuntu
USER root

WORKDIR /app

# Download hadoop-azure jar and its dependency into Spark's jar directory
# so all jobs can access Azure Blob Storage without extra config
RUN apt-get update && apt-get install -y curl && \
    curl -o /opt/spark/jars/hadoop-azure-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar && \
    curl -o /opt/spark/jars/azure-storage-8.6.6.jar \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

USER spark