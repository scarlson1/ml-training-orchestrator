FROM ghcr.io/mlflow/mlflow:v3.11.1
RUN pip install --no-cache-dir 'mlflow[auth]' psycopg2-binary boto3
