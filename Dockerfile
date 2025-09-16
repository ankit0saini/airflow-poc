FROM astrocrpublic.azurecr.io/runtime:3.0-10

# FROM quay.io/astronomer/ap-airflow:2.7.3-1

# Install Java
USER root
RUN apt-get update && apt-get install -y wget gnupg && \
    wget -qO - https://repos.azul.com/azul-repo.key | apt-key add - && \
    echo "deb http://repos.azul.com/zulu/deb stable main" > /etc/apt/sources.list.d/zulu.list && \
    apt-get update && apt-get install -y zulu-11-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/zulu-11-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install PySpark
RUN pip install pyspark
