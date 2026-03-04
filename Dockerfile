FROM python:3.11-slim

WORKDIR /app

COPY weather_prediction/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY weather_prediction/ ./weather_prediction/

CMD ["python", "-m", "weather_prediction.app"]
