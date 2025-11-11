# YouTube Data Analytics Pipeline

## Overview

The **YouTube Data Analytics Pipeline** is a comprehensive, containerized data engineering solution that automates the collection, processing, storage, and visualization of YouTube video and channel statistics. Built with modern big data technologies, this pipeline demonstrates a complete end-to-end data workflow for real-time analytics.

The system continuously monitors YouTube channels, extracts video performance metrics, transforms the data using Apache Spark, stores it in PostgreSQL, and visualizes insights through Grafana dashboards.

### Key Features

- **Automated Data Ingestion**: Hourly collection of YouTube video statistics via the YouTube Data API
- **Distributed Data Processing**: Apache Spark for scalable data transformation and cleaning
- **Containerized Architecture**: Docker and Docker Compose for simplified deployment
- **Workflow Orchestration**: Apache Airflow for reliable DAG-based pipeline scheduling
- **Data Storage**: PostgreSQL database for persistent data management
- **Real-time Visualization**: Grafana dashboards for monitoring and analytics
- **Data Cleaning**: Automatic handling of missing values and null data
- **Derived Metrics**: Calculation of engagement rates, view changes, and performance trends

---

## Project Architecture

### High-Level Data Pipeline

```
YouTube API
    ↓
Airflow DAG (Hourly)
    ↓
Data Ingestion (Python)
    ↓
CSV Storage
    ↓
Apache Spark (PySpark)
    ↓
Data Transformation & Cleaning
    ↓
Derived Metrics Calculation
    ↓
PostgreSQL Database
    ↓
Grafana Dashboards
```

### Component Breakdown

1. **Data Source**: YouTube Data API v3
2. **Orchestration**: Apache Airflow (Scheduled DAGs)
3. **Ingestion**: Python with requests library
4. **Processing**: Apache Spark (Distributed processing)
5. **Storage**: PostgreSQL (Persistent data)
6. **Visualization**: Grafana (Real-time dashboards)

---

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Workflow Orchestration** | Apache Airflow | 2.7.1 | Schedule and monitor DAGs |
| **Database** | PostgreSQL | 15 | Data persistence |
| **Distributed Processing** | Apache Spark | 4.0.0 | Big data transformation |
| **Containerization** | Docker | Latest | Service isolation |
| **Container Orchestration** | Docker Compose | 3.9 | Multi-container management |
| **Data Visualization** | Grafana | Latest | Interactive dashboards |
| **Language** | Python | 3.x | Scripting and data processing |
| **API Client** | requests | Latest | HTTP requests to YouTube API |

---

## Prerequisites

Before setting up the project, ensure you have:

### System Requirements
- **Operating System**: Linux (Ubuntu 20.04+), macOS, or Windows with Docker Desktop
- **RAM**: Minimum 8GB (16GB recommended for Spark)
- **Disk Space**: At least 10GB free space
- **Network**: Stable internet connection for API calls

### Software Requirements
- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 1.29 or higher
- **Git**: For cloning and version control
- **Python**: 3.8+ (for local development)

### External Requirements
- **YouTube Data API Key**: 
  - Create a project in [Google Cloud Console](https://console.cloud.google.com/)
  - Enable the YouTube Data API v3
  - Create an API key credential
  - Store it in a `.env` file (see Configuration section)

---

## Project Structure

```
/Assignment
├── docker-compose.yaml              # Multi-container orchestration
├── Dockerfile                       # Custom Spark image definition
├── .env                            # Environment variables (YouTube API key)
├── README.md                       # This file
│
├── airflow/                        # Apache Airflow configuration and DAGs
│   ├── airflow.cfg                # Airflow configuration file
│   ├── webserver_config.py         # Webserver settings
│   ├── dags/
│   │   └── youtube_api_dags.py     # Main pipeline DAG definition
│   ├── logs/                       # DAG execution logs
│   └── *.csv                       # Extracted video data files
│       ├── video_data.csv          # Historical video statistics
│       ├── new_video_data.csv      # Latest extracted video data
│       └── channel_data.csv        # Channel information
│
├── spark_jobs/
│   └── spark_transformation.py     # PySpark data transformation and aggregation
│
├── spark/
│   └── app/                        # Spark application files
│
├── ingestion.py                    # Standalone data ingestion script
│
├── grafana_insigth_queries.sql     # Pre-built SQL queries for visualizations

```

---

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone https://github.com/kazeric/Yotube_Data_Analytics_pipeline.git
cd Assignment
```

### Step 2: Create Environment File

Create a `.env` file in the project root directory with your YouTube API credentials:

```bash
cat > .env << EOF
YOUTUBE_API_KEY=your_youtube_api_key_here
EOF
```

Replace `your_youtube_api_key_here` with your actual API key from Google Cloud Console.

### Step 3: Verify Docker Installation

Ensure Docker and Docker Compose are installed and running:

```bash
docker --version
docker-compose --version
docker ps
```


### Step 4: Start All Services

```bash
docker-compose up -d
```

This command starts all services in detached mode:
- **PostgreSQL**: Database server
- **Airflow WebServer**: UI at http://localhost:8000
- **Airflow Scheduler**: Schedules DAGs
- **Spark Master**: Distributed processing master
- **Spark Worker**: Distributed processing worker

### Step 5: Verify Services

Check that all containers are running:

```bash
docker-compose ps
```


### Step 7: Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow Web UI** | http://localhost:8000 | admin / admin |
| **Spark Master UI** | http://localhost:8080 | N/A |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

---

## Configuration

### Environment Variables

The `.env` file should contain:

```bash
# YouTube API Configuration
YOUTUBE_API_KEY=your_api_key_here
```


## Running the Pipeline

### Manual Pipeline Execution

#### Via Airflow Web UI

1. Navigate to http://localhost:8000
2. Log in with credentials: `admin` / `admin`
3. Find the DAG named `youtube_data_pipeline`
4. Click the **Play** button to trigger a manual run
5. Monitor the execution in the **Graph** or **Tree** view

#### Via Command Line

```bash
# Trigger the DAG manually
docker-compose exec airflow-webserver airflow dags trigger youtube_data_pipeline

# Check DAG status
docker-compose exec airflow-webserver airflow dags list

# View recent DAG runs
docker-compose exec airflow-webserver airflow dags list-runs -d youtube_data_pipeline
```

### Automated Execution

The pipeline is configured to run **automatically every hour** via the schedule interval:

```python
schedule_interval= '@hourly'
```

To modify the schedule, edit `airflow/dags/youtube_api_dags.py`:

```python
schedule_interval= '@daily'      # Daily execution
schedule_interval= '0 */6 * * *' # Every 6 hours (cron syntax)
schedule_interval= '0 0 * * 0'   # Weekly on Sundays
schedule_interval= '@hourly'     # Default: Hourly
```

---

## Data Flow

### Step-by-Step Data Journey

#### 1. **Data Ingestion Task** (`fetch_data_task`)

The Airflow DAG executes `fetch_data()` function:

```
YouTube API → Python requests library → pandas DataFrame → CSV file
```

**Process**:
- Connects to YouTube Data API using API key
- Searches for latest videos from specified channel: `UCrkzfc2yf-7q6pd7EtzgNaQ`
- Retrieves up to 15 most recent videos
- Extracts metadata: video ID, title, publish date
- Fetches statistics: view count, like count, comment count
- Adds extraction timestamp
- Saves to `/opt/airflow/new_video_data.csv`

**Sample Data**:
```csv
extraction_time,video_id,title,publish_date,view_count,like_count,comment_count
2025-10-09 09:49:44,k64goYZL69E,"Video Title",2025-10-01T12:30:00Z,1500,120,45
```

#### 2. **Data Transformation Task** (`run_spark_task`)

Spark job transforms and enriches the data:

**Process**:
- Reads CSV files into Spark DataFrames
- Cleans missing values using window functions and forward-fill logic
- Calculates derived metrics:
  - `like_rate`: Likes per view
  - `comment_rate`: Comments per view
  - `engagement_rate`: (Likes + Comments) per view
  - `view_change`: Change in view count since last extraction
  - `like_change`: Change in like count since last extraction
  - `comment_change`: Change in comment count since last extraction
- Writes transformed data to PostgreSQL

**Key Transformation**:
```python
video_df = video_df.withColumn("like_rate", col("like_count") / col("view_count"))
video_df = video_df.withColumn("engagement_rate", 
    (col("like_count") + col("comment_count")) / col("view_count"))
```

#### 3. **Data Storage**

Spark writes data to PostgreSQL table: `video_stats`

#### 4. **Visualization**

Grafana queries PostgreSQL and displays real-time dashboards

### Data Dependencies

```
Channel Data (channel_data.csv)
    ↓
Spark Transformation
    ↓
PostgreSQL
    ↓
Video Stats Table

New Video Data (new_video_data.csv)
    ↓
Spark Transformation
    ↓
PostgreSQL
    ↓
Video Stats Table
```
