#!/bin/bash
set -e

echo "--- Upgrading Superset metadata database..."
superset db upgrade

echo "--- Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --firstname "Admin" \
  --lastname "User" \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo "--- Initializing Superset roles and permissions..."
superset init

echo "--- Importing Snowflake connection..."
python /app/snowflake_import.py

echo "--- Superset initialization complete."
