FROM apache/spark:4.0.2-scala2.13-java17-python3-ubuntu
USER root

WORKDIR /app

# Download hadoop-azure 3.4.1 (matching Spark 4.0.2's Hadoop) and dependencies
RUN cd /opt/spark/jars && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.4.1/hadoop-azure-3.4.1.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.53.v20231009/jetty-util-ajax-9.4.53.v20231009.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.53.v20231009/jetty-util-9.4.53.v20231009.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.2.0/hadoop-shaded-guava-1.2.0.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar && \
    curl -fsSL -O https://repo1.maven.org/maven2/commons-codec/commons-codec/1.15/commons-codec-1.15.jar

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

USER spark