#!/bin/bash
# View Airflow logs

cd "$(dirname "$0")"

if [ -z "$1" ]; then
    echo "📝 Viewing all Airflow logs (Ctrl+C to exit)..."
    echo "================================"
    docker-compose logs -f
else
    echo "📝 Viewing $1 logs (Ctrl+C to exit)..."
    echo "================================"
    docker-compose logs -f "$1"
fi


