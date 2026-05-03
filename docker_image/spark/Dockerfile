FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# Cài pip
RUN apt-get update && apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Copy và cài requirements
COPY requirements.txt /opt/spark/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements.txt

# Tạo thư mục
RUN mkdir -p /opt/spark/work-dir /opt/spark/jars/delta

WORKDIR /opt/spark/work-dir

# Sử dụng entrypoint mặc định của Spark (không override)
# Spark sẽ tự chạy master/worker dựa vào biến môi trường SPARK_MODE
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]