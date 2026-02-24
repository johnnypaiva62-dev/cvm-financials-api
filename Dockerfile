FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/cache

# Railway injeta a variável PORT automaticamente
ENV PORT=8000
EXPOSE ${PORT}

CMD ["python", "main.py"]
