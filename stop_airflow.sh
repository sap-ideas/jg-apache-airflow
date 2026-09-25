#!/bin/bash
# Stop Airflow services

set -e

echo "🛑 Stopping Airflow..."
echo "================================"

cd "$(dirname "$0")"

docker-compose down

echo ""
echo "✅ Airflow stopped successfully!"
echo "================================"
echo "To start again: ./start_airflow.sh"


