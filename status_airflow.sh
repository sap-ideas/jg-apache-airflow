#!/bin/bash
# Check Airflow status

cd "$(dirname "$0")"

echo "📊 Airflow Services Status"
echo "================================"
docker-compose ps
echo ""
echo "💾 Docker Volumes"
echo "================================"
docker volume ls | grep repository || echo "No volumes found"


