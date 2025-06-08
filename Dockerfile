FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV PYTHON_VERSION=3.10

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3-pip \
    python3-dev \
    wget \
    curl \
    git \
    unzip \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

# Copy files
WORKDIR /workspace
COPY requirements.txt .
COPY generate_genomic_data.py .

# Install Python packages
RUN pip3 install --no-cache-dir -r requirements.txt \
    jupyterlab \
    pyspark==3.4.1 \
    delta-spark==2.4.0 \
    great_expectations==0.18.13

# Generate the data
RUN python3 generate_genomic_data.py

# Expose Jupyter port
EXPOSE 8887

# Start JupyterLab
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8887", "--allow-root", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]
