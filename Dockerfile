FROM --platform=linux/amd64 flink:1.16.2

# Install Python 3.7.9 from source (PyFlink 1.16 only supports 3.6–3.8)
RUN apt-get update -y && \
    apt-get install -y wget build-essential libssl-dev zlib1g-dev \
                       libbz2-dev libffi-dev liblzma-dev && \
    wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar -xvf Python-3.7.9.tgz && \
    cd Python-3.7.9 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy Python requirements
COPY requirements.txt /opt/requirements.txt

# Install Python dependencies
RUN python -m pip install --upgrade pip && \
    pip3 install -r /opt/requirements.txt --no-cache-dir

# Install Java 11
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download connector JARs (PyFlink, Kafka, JDBC, Postgres)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-python/1.16.2/flink-python-1.16.2.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.2/flink-sql-connector-kafka-1.16.2.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/1.16.2/flink-connector-jdbc-1.16.2.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.26/postgresql-42.2.26.jar

WORKDIR /opt/flink
