# E-commerce Analytics Pipeline

A production-ready data engineering pipeline that processes e-commerce data to provide business insights and analytics. This project demonstrates modern data engineering practices using Apache Airflow, dbt, and PostgreSQL.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │────│   Apache Airflow │────│   PostgreSQL    │────│   dbt Models    │
│                 │    │                  │    │                 │    │                 │
│ • CSV Files     │    │ • ETL Pipelines  │    │ • Raw Layer     │    │ • Staging       │
│ • APIs          │    │ • Orchestration  │    │ • Staging       │    │ • Warehouse     │
│ • Databases     │    │ • Monitoring     │    │ • Warehouse     │    │ • Marts         │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
                                                                               │
                       ┌─────────────────┐    ┌─────────────────┐             │
                       │   Monitoring    │    │   Business      │◄────────────┘
                       │                 │    │   Intelligence  │
                       │ • Data Quality  │    │                 │
                       │ • Alerting      │    │ • Dashboards    │
                       │ • Lineage       │    │ • Reports       │
                       └─────────────────┘    └─────────────────┘
```

## ✨ Key Features

- **🔄 Automated ETL Pipeline**: Daily batch processing with Apache Airflow
- **📊 Data Modeling**: Dimensional modeling with dbt (staging → warehouse → marts)
- **🧪 Data Quality**: Comprehensive testing and validation
- **📈 Business Metrics**: Customer analytics, product performance, and KPIs
- **🐳 Containerized**: Full Docker-based deployment
- **📚 Documentation**: Auto-generated dbt documentation
- **🔍 Monitoring**: Data quality monitoring and alerting

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- 8GB+ RAM recommended

### 1. Clone and Deploy

```bash
git clone <repository-url>
cd ecommerce-analytics-pipeline

# Make deployment script executable
chmod +x deploy.sh

# Deploy the pipeline
./deploy.sh dev
```

### 2. Access the Platform

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Database**: localhost:5433 (warehouse/warehouse)

### 3. Generate Documentation

```bash
# Generate dbt docs
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt docs generate && dbt docs serve --port 8081"

# Access at http://localhost:8081
```

## 📁 Project Structure

```
ecommerce-analytics-pipeline/
├── airflow/
│   ├── dags/                     # Airflow DAGs
│   │   └── ecommerce_etl_dag.py  # Main ETL pipeline
│   ├── plugins/                  # Custom operators
│   └── logs/                     # Airflow logs
├── dbt/
│   ├── models/
│   │   ├── staging/              # Data cleaning & validation
│   │   ├── warehouse/            # Dimensional models
│   │   └── marts/                # Business-ready aggregations
│   ├── tests/                    # Custom dbt tests
│   ├── macros/                   # Reusable SQL macros
│   └── dbt_project.yml          # dbt configuration
├── data-generation/
│   └── generate_ecommerce_data.py # Sample data generator
├── infrastructure/
│   ├── docker-compose.yml        # Docker services
│   ├── Dockerfile                # Custom Airflow image
│   └── warehouse-init.sql        # Database schema
├── tests/                        # Integration tests
├── docs/                         # Project documentation
├── deploy.sh                     # Deployment script
└── README.md                     # This file
```

## 🔄 Data Pipeline

### Data Flow

1. **Raw Layer**: Ingested data from various sources
2. **Staging Layer**: Cleaned and validated data
3. **Warehouse Layer**: Dimensional models (facts & dimensions)
4. **Marts Layer**: Business-ready aggregated data

### Key Models

#### Staging Models
- `stg_customers`: Cleaned customer data with derived attributes
- `stg_orders`: Order data with enrichments and validation
- `stg_products`: Product catalog with calculated metrics
- `stg_order_items`: Order line items with validations

#### Warehouse Models
- `dim_customers`: Customer dimension with SCD Type 2
- `dim_products`: Product dimension with attributes
- `dim_date`: Date dimension with business calendar
- `fact_orders`: Order transactions fact table
- `fact_order_items`: Order line items fact table

#### Mart Models
- `customer_summary`: Customer 360 view with RFM analysis
- `product_performance`: Product analytics and recommendations
- `sales_summary`: Sales performance metrics

## 📊 Business Metrics

The pipeline provides insights into:

### Customer Analytics
- Customer Lifetime Value (CLV)
- RFM Segmentation (Recency, Frequency, Monetary)
- Customer Health Score
- Churn Prediction
- Cohort Analysis

### Product Performance
- Revenue and profit analysis
- Inventory turnover
- Return rate analysis
- Cross-sell opportunities
- Product health scoring

### Operational Metrics
- Daily/Monthly/Quarterly revenue
- Order fulfillment metrics
- Payment method analysis
- Geographic performance

## 🧪 Data Quality

### Automated Testing
- **Source freshness**: Ensures data is loaded within SLA
- **Referential integrity**: Foreign key relationships
- **Data validation**: Range checks, null checks, uniqueness
- **Custom business rules**: Revenue reconciliation, customer consistency

### Quality Monitoring
- Daily data quality reports
- Automated alerting on failures
- Data lineage tracking
- Performance monitoring

## 🛠️ Development

### Adding New Models

1. Create SQL files in appropriate dbt directory
2. Add tests in `schema.yml` files
3. Update DAG dependencies if needed
4. Test locally: `dbt run --models new_model`

### Running Tests

```bash
# dbt tests
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test"

# Airflow DAG tests
docker-compose exec airflow-webserver airflow dags test ecommerce_etl_pipeline
```

### Debugging

```bash
# View Airflow logs
docker-compose logs -f airflow-scheduler

# Check database
docker-compose exec warehouse psql -U warehouse -d ecommerce_dw

# dbt debug
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug"
```

## 📈 Performance Optimization

### Database Optimizations
- Proper indexing on foreign keys and filter columns
- Partitioning on date columns for large fact tables
- Column compression for storage efficiency

### Pipeline Optimizations
- Incremental dbt models for large datasets
- Parallel task execution in Airflow
- Resource allocation tuning

### Query Performance
- Materialized views for frequently accessed data
- Pre-aggregated summary tables
- Query result caching

## 🔐 Security & Compliance

- Environment-based configuration
- Database connection encryption
- PII data handling guidelines
- Audit logging for data access

## 🚀 Production Deployment

### Environment Configuration

```bash
# Production deployment
./deploy.sh prod
```

### Monitoring Setup
- Set up alerts for DAG failures
- Monitor data freshness SLAs
- Track resource utilization
- Set up log aggregation

### Backup Strategy
- Database backups
- Configuration version control
- Disaster recovery procedures

## 📚 Documentation

- **dbt Docs**: Auto-generated data catalog and lineage
- **Architecture Diagrams**: Visual system overview
- **Runbooks**: Operational procedures
- **API Documentation**: For data access endpoints

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📋 Roadmap

- [ ] Real-time streaming pipeline
- [ ] ML model integration
- [ ] Cloud deployment (AWS/GCP)
- [ ] Advanced alerting system
- [ ] API layer for data access
- [ ] Advanced visualization dashboards

## 🆘 Troubleshooting

### Common Issues

**Airflow webserver won't start**
```bash
# Check logs
docker-compose logs airflow-webserver

# Reset Airflow database
docker-compose exec airflow-webserver airflow db reset
```

**dbt models fail**
```bash
# Check dbt logs
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --debug"

# Validate configuration
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug"
```

**Database connection issues**
```bash
# Test warehouse connection
docker-compose exec warehouse pg_isready -U warehouse

# Check network connectivity
docker-compose exec airflow-webserver ping warehouse
```

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review Airflow and dbt logs
3. Create an issue with detailed error information

---

**Built with ❤️ for modern data engineering practices**