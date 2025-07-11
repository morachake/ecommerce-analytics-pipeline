#!/bin/bash

# Complete project setup script
# This script creates all directories and files for the e-commerce analytics pipeline

echo "🚀 Creating complete E-commerce Analytics Pipeline project..."

# Create main project directory
PROJECT_NAME="ecommerce-analytics-pipeline"
mkdir -p $PROJECT_NAME
cd $PROJECT_NAME

# Create directory structure
echo "📁 Creating directory structure..."
mkdir -p {airflow/{dags,plugins,logs},dbt/{models/{staging,warehouse,marts},tests,macros,seeds,snapshots,analyses},data-generation,infrastructure,docs,tests}

# Create data directory
mkdir -p data

echo "✅ Directory structure created!"

# Generate Fernet key for Airflow
echo "🔑 Generating Fernet key..."
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || echo "your-fernet-key-here-replace-with-generated-key")

# Create .env file
echo "📝 Creating .env file..."
cat > .env << EOF
# Airflow Configuration
AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

WAREHOUSE_USER=warehouse
WAREHOUSE_PASSWORD=warehouse
WAREHOUSE_DB=ecommerce_dw

# dbt Configuration
DBT_TARGET=dev
EOF

echo "✅ Configuration files created!"
echo ""
echo "📋 Next steps:"
echo "1. Copy all the provided code files into their respective directories"
echo "2. Install prerequisites: Docker, Python 3.8+, Git"
echo "3. Run: python3 verify_setup.py"
echo "4. Run: ./deploy.sh dev"
echo ""
echo "📂 Project structure created in: $(pwd)"
