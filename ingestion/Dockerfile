FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# no default CMD — the command is passed at runtime
# by docker-compose for consumers and by Airflow for producers