FROM python:3.11-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

# Copy requirements trước để tận dụng cache layer
COPY etl_pipeline/requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Cài thêm dagster-deltalake (nếu chưa có trong requirements)
RUN pip install dagster-deltalake

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]