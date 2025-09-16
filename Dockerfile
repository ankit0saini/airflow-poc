FROM astrocrpublic.azurecr.io/runtime:3.0-10

# FROM quay.io/astronomer/ap-airflow:2.7.3-1

# Download required Hadoop Azure JARs for ADLS support
RUN curl -L -o /opt/spark-jars/hadoop-azure-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar && \
    curl -L -o /opt/spark-jars/azure-storage-8.6.6.jar https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar

ENV SPARK_JARS_DIR=/opt/spark-jars
ENV PYSPARK_SUBMIT_ARGS="--jars ${SPARK_JARS_DIR}/hadoop-azure-3.3.1.jar,${SPARK_JARS_DIR}/azure-storage-8.6.6.jar pyspark-shell"

# Install PySpark
RUN pip install pyspark
