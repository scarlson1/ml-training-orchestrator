FROM ghcr.io/mlflow/mlflow:v3.11.1
RUN pip install --no-cache-dir 'mlflow[auth]' psycopg2-binary boto3
COPY infra/docker/mlflow-entrypoint.sh /usr/local/bin/mlflow-entrypoint.sh
RUN chmod +x /usr/local/bin/mlflow-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/mlflow-entrypoint.sh"]