#!/bin/sh
set -eu

cat > /mlflow_auth/basic_auth.ini <<EOF
[mlflow]
default_permission = READ
database_uri = sqlite:////mlflow_auth/basic_auth.db
admin_username = ${MLFLOW_AUTH_ADMIN_USERNAME}
admin_password = ${MLFLOW_AUTH_ADMIN_PASSWORD}
authorization_function = mlflow.server.auth:authenticate_request_basic_auth
EOF

exec "$@"
