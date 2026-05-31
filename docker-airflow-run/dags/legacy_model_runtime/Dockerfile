FROM python:3.8-slim

WORKDIR /opt/legacy_model_runtime

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY legacy_model_runner.py .
