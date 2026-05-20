FROM python:3.11-slim
 
WORKDIR /app
 
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*
 
COPY evaluation/requirements_eval.txt .
RUN pip install --no-cache-dir -r requirements_eval.txt
 
COPY evaluation/ /app/evaluation/
 
CMD ["python", "evaluation/run_evaluation.py"]