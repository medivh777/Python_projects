FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pgmon ./pgmon
COPY config.example.yaml .

ENV PGMON_CONFIG=/app/config.yaml
EXPOSE 8080

# по умолчанию — веб; коллектор переопределяет command в compose
CMD ["uvicorn", "pgmon.web.app:app", "--host", "0.0.0.0", "--port", "8080"]
