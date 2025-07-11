#!/bin/bash

# Script to create .env file with proper Fernet key generation

echo "🔑 Generating .env file with Fernet key..."

# Check if Python and cryptography are available
if python3 -c "from cryptography.fernet import Fernet" 2>/dev/null; then
    echo "✅ Cryptography library found"
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
else
    echo "📦 Installing cryptography library..."
    pip3 install cryptography
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
fi

# Create .env file
cat > .env << EOF
# Airflow Configuration
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Airflow Database Configuration (runs in Docker)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Data Warehouse Configuration (runs in Docker)
WAREHOUSE_USER=warehouse
WAREHOUSE_PASSWORD=warehouse
WAREHOUSE_DB=ecommerce_dw

# dbt Configuration
DBT_TARGET=dev
EOF

echo "✅ .env file created successfully!"
echo ""
echo "🔑 Generated Fernet Key: ${FERNET_KEY}"
echo ""
echo "📋 Configuration Summary:"
echo "- Airflow will run on: http://localhost:8080"
echo "- Airflow DB will run on: localhost:5432 (internal to Docker)"
echo "- Warehouse DB will run on: localhost:5433"
echo "- All services run in Docker containers - no local installation needed!"
echo ""
echo "🚀 Next step: Run './deploy.sh dev' to start all services"