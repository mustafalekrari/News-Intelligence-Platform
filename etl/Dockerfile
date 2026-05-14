FROM --platform=linux/arm64 python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt'); nltk.download('punkt_tab')"
COPY . .
# Rendre le module quality accessible
ENV PYTHONPATH="/app:/app/quality"
CMD ["python", "worker.py"]
