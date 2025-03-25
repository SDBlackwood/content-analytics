FROM python:3.12-slim

WORKDIR /app

# Install dependencies from requirements file
COPY --chown=airflow:root docker/workflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set up application
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "-m", "content_analytics.bin.generate_events"] 