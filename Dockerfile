FROM apache/airflow:2.7.2-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy Airflow-specific requirements
COPY requirements-airflow.txt .

# Install Python packages with error handling
RUN pip install --no-cache-dir -r requirements-airflow.txt || \
    pip install --no-cache-dir \
    dbt-postgres==1.6.0 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    psycopg2-binary==2.9.7 \
    apache-airflow-providers-postgres==5.7.1

# Note: We don't initialize Airflow DB here - it's done at runtime by the deploy script
# Initialize Airflow database and create admin user
# RUN airflow db init && \
#     airflow users create \
#     --username admin \
#     --firstname Admin \
#     --lastname User \
#     --role Admin \
#     --email admin@example.com \
#     --password admin