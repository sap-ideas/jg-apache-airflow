#!/bin/bash
# Start Airflow with Docker Compose

set -e

echo "🚀 Starting Airflow..."
echo "================================"

cd "$(dirname "$0")"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    echo "   Run: sudo service docker start"
    exit 1
fi

# Check if this is first time setup
if [ ! -d "logs" ] || [ -z "$(ls -A logs)" ]; then
    echo "📦 First time setup detected..."
    echo "   Initializing Airflow database..."
    docker-compose up airflow-init
    echo "✅ Initialization complete!"
    echo ""
fi

# Start all services
echo "🔄 Starting Airflow services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 10

# Install Python dependencies (requirements.txt mounted at /opt/airflow/requirements.txt)
echo "📦 Installing Python dependencies..."
if docker-compose exec -T airflow-scheduler test -f /opt/airflow/requirements.txt 2>/dev/null; then
  docker-compose exec -T airflow-webserver python -m pip install -q -r /opt/airflow/requirements.txt
  docker-compose exec -T airflow-scheduler python -m pip install -q -r /opt/airflow/requirements.txt
else
  echo "   ⚠️  /opt/airflow/requirements.txt not found in container — add volume in docker-compose.yaml"
fi

echo ""
echo "✅ Airflow is running!"
echo "================================"
echo "🌐 Web UI: http://localhost:8080"
echo "👤 Username/password: see _AIRFLOW_WWW_USER_USERNAME / _AIRFLOW_WWW_USER_PASSWORD in .env"
echo ""
echo "📊 Check status: docker-compose ps"
echo "📝 View logs: docker-compose logs -f"
echo "🛑 Stop Airflow: ./stop_airflow.sh"
echo "================================"


