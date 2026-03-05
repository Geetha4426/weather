FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Repo root IS the weather package — copy everything into /app/weather/
COPY . ./weather/

CMD ["python", "-m", "weather.app"]
