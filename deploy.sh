#!/bin/bash

# E-commerce Analytics Pipeline Deployment Script with proper Airflow initialization
# Usage: ./deploy.sh [environment]
# Environment: dev (default) or prod

set -e  # Exit on any error

ENVIRONMENT=${1:-dev}
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Deploying E-commerce Analytics Pipeline to $ENVIRONMENT environment"
echo "Project directory: $PROJECT_DIR"

# Check prerequisites
check_prerequisites() {
    echo "✅ Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed"
        echo "Please install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose is not installed"
        echo "Please install Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        echo "❌ Docker daemon is not running"
        echo "Please start Docker daemon"
        exit 1
    fi
    
    echo "✅ Prerequisites check passed"
}

# Setup environment with proper Fernet key
setup_environment() {
    echo "🔧 Setting up $ENVIRONMENT environment..."
    
    # Check if .env file exists and validate Fernet key
    if [ ! -f .env ]; then
        echo "📝 Creating .env file..."
        
        # Generate proper Fernet key
        if command -v python3 &> /dev/null; then
            FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null)
            if [ -z "$FERNET_KEY" ]; then
                echo "❌ Failed to generate Fernet key"
                exit 1
            fi
        else
            echo "❌ Python3 not found. Cannot generate Fernet key."
            exit 1
        fi
        
        cat > .env << EOF
# Airflow Configuration
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
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
DBT_TARGET=${ENVIRONMENT}
EOF
        echo "✅ .env file created with Fernet key: ${FERNET_KEY}"
    else
        # Check if existing .env has proper Fernet key
        CURRENT_KEY=$(grep "AIRFLOW__CORE__FERNET_KEY=" .env | cut -d'=' -f2)
        if [[ "$CURRENT_KEY" == "your-fernet-key-here"* ]] || [ -z "$CURRENT_KEY" ]; then
            echo "🔑 Updating invalid Fernet key in .env..."
            NEW_FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
            sed -i.bak "s/AIRFLOW__CORE__FERNET_KEY=.*/AIRFLOW__CORE__FERNET_KEY=${NEW_FERNET_KEY}/" .env
            echo "✅ Updated Fernet key: ${NEW_FERNET_KEY}"
        else
            echo "✅ .env file exists with valid Fernet key"
        fi
    fi
    
    # Create necessary directories
    mkdir -p {airflow/logs,data,dbt/target,dbt/logs}
    
    echo "✅ Environment setup completed"
}

# Generate sample data
generate_data() {
    echo "📊 Checking for sample data..."
    
    # Create data directory
    mkdir -p data
    
    # Check if data exists
    if [ ! -f data/customers.csv ]; then
        echo "📈 Generating sample e-commerce data..."
        
        # Check if data generation script exists
        if [ -f data-generation/generate_ecommerce_data.py ]; then
            cd data-generation
            
            # Check if required Python packages are installed
            if python3 -c "import pandas, numpy, faker" 2>/dev/null; then
                python3 generate_ecommerce_data.py
                echo "✅ Sample data generated successfully"
            else
                echo "📦 Installing required Python packages..."
                pip3 install pandas numpy faker
                python3 generate_ecommerce_data.py
                echo "✅ Sample data generated successfully"
            fi
            
            cd ..
        else
            echo "⚠️  Data generation script not found. Creating placeholder files..."
            # Create minimal CSV files for testing
            echo "customer_id,first_name,last_name,email" > data/customers.csv
            echo "1,John,Doe,john@example.com" >> data/customers.csv
            
            echo "product_id,product_name,price" > data/products.csv
            echo "1,Sample Product,29.99" >> data/products.csv
            
            echo "order_id,customer_id,order_date,total_amount" > data/orders.csv
            echo "1,1,2024-01-01,29.99" >> data/orders.csv
            
            echo "order_item_id,order_id,product_id,quantity" > data/order_items.csv
            echo "1,1,1,1" >> data/order_items.csv
            
            echo "event_id,customer_id,event_type,event_timestamp" > data/web_events.csv
            echo "1,1,page_view,2024-01-01 10:00:00" >> data/web_events.csv
            
            echo "✅ Placeholder data files created"
        fi
    else
        echo "✅ Sample data already exists"
    fi
}

# Build and start services with proper initialization
start_services() {
    echo "🐳 Building and starting Docker services..."
    
    # Stop any existing services
    echo "🛑 Stopping any existing services..."
    docker-compose down -v --remove-orphans 2>/dev/null || true
    
    # Build images
    echo "🔨 Building Docker images..."
    docker-compose build --no-cache
    
    # Start databases first
    echo "🗄️ Starting databases..."
    docker-compose up -d postgres warehouse redis
    
    # Wait for databases to be healthy
    echo "⏳ Waiting for databases to be ready..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if docker-compose exec -T postgres pg_isready -U airflow 2>/dev/null && \
           docker-compose exec -T warehouse pg_isready -U warehouse 2>/dev/null; then
            echo "✅ Databases are ready!"
            break
        fi
        echo "Waiting for databases... ($((60-timeout))s)"
        sleep 2
        timeout=$((timeout-2))
    done
    
    if [ $timeout -le 0 ]; then
        echo "❌ Databases failed to start"
        return 1
    fi
    
    # Initialize Airflow database
    echo "🔧 Initializing Airflow database..."
    docker-compose run --rm airflow-webserver airflow db init
    
    # Create admin user
    echo "👤 Creating Airflow admin user..."
    docker-compose run --rm airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    
    # Start Airflow services
    echo "🚀 Starting Airflow services..."
    docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
    
    # Wait for Airflow webserver
    echo "⏳ Waiting for Airflow webserver..."
    timeout=120
    while [ $timeout -gt 0 ]; do
        if curl -f http://localhost:8080/health &> /dev/null; then
            echo "✅ Airflow webserver is ready!"
            break
        fi
        echo "Waiting for Airflow webserver... ($((120-timeout))s)"
        sleep 5
        timeout=$((timeout-5))
    done
    
    if [ $timeout -le 0 ]; then
        echo "❌ Airflow webserver failed to start"
        echo "Check logs: docker-compose logs airflow-webserver"
        return 1
    fi
    
    echo "✅ All services are ready"
}

# Initialize dbt
init_dbt() {
    echo "🗄️ Initializing dbt..."
    
    # Wait a bit more for Airflow to fully initialize
    sleep 10
    
    # Install dbt packages
    echo "📦 Installing dbt packages..."
    docker-compose exec -T airflow-webserver bash -c "cd /opt/airflow/dbt && dbt deps" || true
    
    # Run initial dbt setup
    echo "🔧 Running initial dbt models..."
    docker-compose exec -T airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --models staging" || echo "⚠️  Staging models failed, continuing..."
    docker-compose exec -T airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --models warehouse" || echo "⚠️  Warehouse models failed, continuing..."
    docker-compose exec -T airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --models marts" || echo "⚠️  Marts models failed, continuing..."
    
    echo "✅ dbt initialization completed"
}

# Load sample data
load_sample_data() {
    echo "📥 Loading sample data..."
    
    # Copy data files to container
    echo "📋 Copying data files to Airflow container..."
    docker-compose exec -T airflow-webserver mkdir -p /opt/airflow/data
    
    # Copy each data file
    for file in data/*.csv; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            docker cp "$file" $(docker-compose ps -q airflow-webserver):/opt/airflow/data/
            echo "  ✅ Copied $filename"
        fi
    done
    
    # Trigger the initial DAG run
    echo "🎯 Triggering initial ETL pipeline..."
    docker-compose exec -T airflow-webserver airflow dags unpause ecommerce_etl_pipeline || true
    docker-compose exec -T airflow-webserver airflow dags trigger ecommerce_etl_pipeline || true
    
    echo "✅ Sample data loading initiated"
}

# Verify deployment
verify_deployment() {
    echo "🔍 Verifying deployment..."
    
    # Check service health
    echo "Checking service health..."
    
    # Check Airflow
    if curl -f http://localhost:8080/health &> /dev/null; then
        echo "✅ Airflow is healthy (http://localhost:8080)"
    else
        echo "❌ Airflow health check failed"
        echo "Check logs: docker-compose logs airflow-webserver"
        return 1
    fi
    
    # Check database connections
    if docker-compose exec -T warehouse psql -U warehouse -d ecommerce_dw -c "SELECT 1;" &> /dev/null; then
        echo "✅ Warehouse database is accessible"
    else
        echo "❌ Warehouse database connection failed"
        echo "Check logs: docker-compose logs warehouse"
        return 1
    fi
    
    # Check if dbt can connect
    if docker-compose exec -T airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug" &> /dev/null; then
        echo "✅ dbt connection is working"
    else
        echo "⚠️  dbt connection check failed, but this might be normal on first run"
    fi
    
    echo "✅ Deployment verification completed"
}

# Display access information
show_access_info() {
    echo ""
    echo "🎉 Deployment completed successfully!"
    echo ""
    echo "📊 Access Information:"
    echo "============================================"
    echo "🌐 Airflow UI:        http://localhost:8080"
    echo "   Username:          admin"
    echo "   Password:          admin"
    echo ""
    echo "🗄️ Warehouse DB:      localhost:5435"
    echo "   Database:          ecommerce_dw"
    echo "   Username:          warehouse"
    echo "   Password:          warehouse"
    echo ""
    echo "📈 Key DAGs:"
    echo "   - ecommerce_etl_pipeline: Main ETL pipeline"
    echo ""
    echo "🔧 Useful Commands:"
    echo "   - View logs:       docker-compose logs -f [service_name]"
    echo "   - Stop services:   docker-compose down"
    echo "   - Restart:         docker-compose restart"
    echo "   - Connect to DB:   docker-compose exec warehouse psql -U warehouse -d ecommerce_dw"
    echo ""
    echo "📁 Generate dbt docs:"
    echo "   docker-compose exec airflow-webserver bash -c 'cd /opt/airflow/dbt && dbt docs generate && dbt docs serve --port 8081'"
    echo "   Then visit: http://localhost:8081"
    echo ""
}

# Cleanup function
cleanup_on_error() {
    echo "❌ Deployment failed. Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    echo "🧹 Cleanup completed"
    echo ""
    echo "🔍 Troubleshooting tips:"
    echo "1. Check Docker is running: docker info"
    echo "2. Check ports are free: netstat -an | grep -E ':(5434|5435|6379|8080)'"
    echo "3. Check logs: docker-compose logs"
    echo "4. Try again: ./deploy.sh dev"
    exit 1
}

# Main deployment flow
main() {
    # Set error handler
    trap cleanup_on_error ERR
    
    echo "🚀 Starting deployment process for $ENVIRONMENT environment..."
    echo ""
    
    check_prerequisites
    setup_environment
    generate_data
    start_services
    init_dbt
    load_sample_data
    verify_deployment
    show_access_info
    
    echo "🏁 Deployment process completed successfully!"
    echo ""
    echo "🎯 Next steps:"
    echo "1. Visit http://localhost:8080 to access Airflow"
    echo "2. Monitor the 'ecommerce_etl_pipeline' DAG execution"
    echo "3. Explore your data warehouse at localhost:5435"
}

# Run main function with all arguments
main "$@"