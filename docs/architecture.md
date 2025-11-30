# Telecom Fraud Detection Pipeline - Architecture

## High-Level Data Flow
This project uses the **Lambda Architecture** to handle both real-time fraud detection and historical analysis.

```mermaid
graph LR
    %% Nodes
    Source[Python Producer\n(Simulates Calls)] -->|Stream| Kafka[Kafka Cluster\n(3 Brokers)]
    
    %% Speed Layer
    Kafka -->|Read| SparkStream[Spark Streaming\n(Running on WSL2)]
    SparkStream -->|Alerts| ClickHouse[(ClickHouse DB)]
    
    %% Batch Layer
    Kafka -->|Archive| HDFS[HDFS Storage]
    HDFS -->|Read| SparkBatch[Spark Batch Job\n(Running on WSL2)]
    SparkBatch -->|Stats| ClickHouse
    
    %% Orchestration
    Airflow[Apache Airflow] -->|Trigger| SparkBatch
    
    %% Serving
    ClickHouse -->|Visualize| Grafana[Grafana Dashboard]
```

## Component Breakdown

### 1. Source Layer
*   **Data**: CSV files (CDRs, Complaints, etc.).
*   **Tool**: `producer.py` (Python).
*   **Role**: Simulates a live stream of phone calls by reading CSVs and sending them to Kafka.

### 2. Ingestion Layer (Docker)
*   **Tool**: **Apache Kafka** (3 Brokers).
*   **Role**: The central nervous system. It buffers data so nothing is lost, even if processing is slow.
*   **Configuration**: 3 Partitions, Replication Factor 3 for high availability.

### 3. Processing Layer (WSL2)
*   **Tool**: **Apache Spark** (PySpark).
*   **Why WSL2?**: Running Spark natively in Linux (WSL2) saves RAM compared to running it in Docker, while still providing a production-like environment.
*   **Speed Job**: Reads from Kafka, detects fraud in real-time, writes to ClickHouse.
*   **Batch Job**: Reads from HDFS, calculates daily stats, writes to ClickHouse.

### 4. Storage Layer (Docker)
*   **Batch Storage**: **HDFS** (Hadoop). Stores the raw "Master Dataset" forever.
*   **Serving Storage**: **ClickHouse**. A super-fast analytical database for powering the dashboard.

### 5. Orchestration (Docker)
*   **Tool**: **Apache Airflow**.
*   **Role**: Wakes up every night to run the Batch Job.

### 6. Visualization (Docker)
*   **Tool**: **Grafana**.
*   **Role**: Displays real-time alerts and historical trends from ClickHouse.
