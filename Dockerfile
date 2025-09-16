FROM astrocrpublic.azurecr.io/runtime:3.0-10

# FROM quay.io/astronomer/ap-airflow:2.7.3-1

# Install Java
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install PySpark
RUN pip install pyspark
