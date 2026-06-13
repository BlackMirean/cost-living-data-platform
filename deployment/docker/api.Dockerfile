FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend ./backend
COPY database ./database

ENV PYTHONPATH=/app
ENV ELASTICSEARCH_URL=http://elasticsearch:9200
ENV RAW_POSTS_INDEX=cost_living_raw_posts
ENV POSTS_INDEX=cost_living_posts_current
ENV INDICATORS_INDEX=cost_living_indicators

EXPOSE 8000

CMD ["uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
