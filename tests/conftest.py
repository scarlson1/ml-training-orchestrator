"""
Set required env vars at module load time — before any bmo import triggers
Settings() instantiation. Fixtures that need these values can rely on them
already being present in os.environ.
"""

import os

os.environ.setdefault('S3_ENDPOINT_URL', 'http://localhost:9000')
os.environ.setdefault('S3_ACCESS_KEY_ID', 'admin')
os.environ.setdefault('S3_SECRET_ACCESS_KEY', 'password123')
os.environ.setdefault('MLFLOW_TRACKING_URI', 'http://localhost:5000')
