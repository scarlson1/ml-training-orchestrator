#!/usr/bin/env bash
set -euo pipefail

ENDPOINT='http://localhost:9000'
AWS_ACCESS_KEY_ID='admin'
AWS_SECRET_ACCESS_KEY='password123'
AWS_DEFAULT_REGION='us-east-1'

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION

BUCKETS=(raw staging rejected mlflow-artifacts)

echo '==> Waiting for MinIO to be ready...'
until curl -sf "${ENDPOINT}/minio/health/live" > /dev/null 2>&1; do
    printf '.'
    sleep 1
done
echo ' ready.'

echo '==> Creating buckets...'
for bucket in "${BUCKETS[@]}"; do
    if aws s3 ls "s3://${bucket}" --endpoint-url "$ENDPOINT" > /dev/null 2>&1; then
        echo "    s3://${bucket} already exists, skipping"
    else
        aws s3 mb "s3://${bucket}" --endpoint-url "$ENDPOINT"
        echo "    created s3://${bucket}"
    fi
done

echo '==> Done. Buckets:'
aws s3 ls --endpoint-url "$ENDPOINT"
