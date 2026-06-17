FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501
CMD ["streamlit", "run", "1_💬_Trợ_lý_AI.py", "--server.address=0.0.0.0", "--server.port=8501"]