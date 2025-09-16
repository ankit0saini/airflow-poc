# FROM astrocrpublic.azurecr.io/runtime:3.0-10
# Use Astronomer runtime as base
FROM astrocrpublic.azurecr.io/runtime:3.0-10

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    gnupg \
    unzip \
    && apt-get clean

# Install AdoptOpenJDK 11 (works on slim images)
RUN curl -fsSL https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.21+9/OpenJDK11U-jre_x64_linux_hotspot_11.0.21_9.tar.gz -o /tmp/jdk.tar.gz \
    && mkdir -p /usr/lib/jvm \
    && tar -xzf /tmp/jdk.tar.gz -C /usr/lib/jvm \
    && rm /tmp/jdk.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/jdk-11.0.21+9/jdk-11.0.21+9
ENV PATH=$JAVA_HOME/bin:$PATH

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.0

USER astro
