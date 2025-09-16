# FROM astrocrpublic.azurecr.io/runtime:3.0-10
FROM astrocrpublic.azurecr.io/runtime:3.0-10-buster

# FROM quay.io/astronomer/ap-airflow:2.7.3-1

# Install Java
USER root
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/zulu-11-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install PySpark
RUN pip install pyspark
