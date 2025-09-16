FROM astrocrpublic.azurecr.io/runtime:3.0-10

# FROM quay.io/astronomer/ap-airflow:2.7.3-1

# Install PySpark
RUN pip install pyspark
