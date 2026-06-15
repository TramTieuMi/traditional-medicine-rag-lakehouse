FROM python:3.11-slim

# Java cần cho PySpark driver (kết nối spark://spark-master:7077)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

COPY requirements.txt .

RUN pip install --no-cache-dir torch==2.6.0+cpu \
    --extra-index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r requirements.txt

# Layer riêng để tránh bị cache khi requirements.txt chưa đổi
RUN pip install --no-cache-dir pymongo==4.6.2

COPY . .

# dagster code-server hỗ trợ hot-reload khi file thay đổi (dagster api grpc thì không)
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
