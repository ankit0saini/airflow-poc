FROM astrocrpublic.azurecr.io/runtime:3.0-10

# FROM quay.io/astronomer/ap-airflow:2.7.3-1

# Install Java
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Download required Hadoop Azure JARs for ADLS support
RUN curl -L -o /opt/spark-jars/hadoop-azure-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar && \
    curl -L -o /opt/spark-jars/azure-storage-8.6.6.jar https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar

# Install PySpark
RUN pip install pyspark
