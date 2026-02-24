FROM python:3.12-slim

WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código
COPY . .

# Cria diretório de cache
RUN mkdir -p data/cache

# Porta (Railway usa $PORT)
EXPOSE 8000

# Startup: uvicorn
CMD ["python", "main.py", "--host", "0.0.0.0", "--port", "8000"]
