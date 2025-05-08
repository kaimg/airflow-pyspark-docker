# Airflow PySpark Docker Project

This project sets up an Apache Airflow environment with PySpark support using Docker containers.

## Prerequisites

- Docker
- Docker Compose
- Git

## Project Structure

```
.
├── dags/                   # Airflow DAG files
├── plugins/                # Airflow plugins
├── logs/                   # Airflow logs
├── config/                 # Configuration files
├── scripts/               # Utility scripts
├── docker-compose.yaml    # Docker Compose configuration
└── Dockerfile-airflow     # Airflow Dockerfile
```

## Setup Instructions

1. Clone the repository:
   ```bash
   git clone https://github.com/kaimg/airflow-pyspark-docker.git
   cd airflow-pyspark-docker
   ```

2. Start the containers:
   ```bash
   docker-compose up -d
   ```

3. Access Airflow web interface:
   - Open your browser and navigate to `http://localhost:8080`
   - Default login credentials:
     - Username: airflow
     - Password: airflow

## Development

- Place your DAG files in the `dags/` directory
- Add custom plugins to the `plugins/` directory
- Configuration files should go in the `config/` directory