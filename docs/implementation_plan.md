# Telecom Fraud Detection Pipeline - Implementation Plan

## Architecture Overview
**Lambda Architecture** combining real-time (Speed Layer) and batch processing (Batch Layer) with KRaft-based Kafka, automated HDFS ingestion via Kafka Connect, and ClickHouse serving layer.

---

## Phase 1: Infrastructure (Docker)

### Kafka Cluster (KRaft Mode)
- **3 Brokers**: `kafka-1:9092`, `kafka-2:9093`, `kafka-3:9094`
- **No Zookeeper**: Self-managed consensus via KRaft protocol
- **Replication**: Factor 3 for all topics

### Kafka Connect
- **HDFS Sink Connector**: Auto-archives data from 11 topics
- **Format**: Parquet (partitioned by date)
- **Management**: REST API on port 8083

### HDFS Cluster
- **1 Namenode** + **3 Datanodes**
- **Replication Factor**: 3
- **Web UI**: http://localhost:9870

### ClickHouse
- **Tables**: `fraud_alerts`, [daily_stats](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/batch_processor.py#56-185)
- **Credentials**: admin/admin123
- **Ports**: HTTP 8123, Native 9001

### Grafana
- **Port**: 3000
- **Data Source**: ClickHouse

---

## Phase 2: Source System

### Producer ([src/source/producer.py](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/source/producer.py))
- Reads 11 CSV files from `Data/Source_System_Data/`
- Streams to Kafka topics in round-robin
- Cross-platform: Windows & WSL support

### Topics
`telecom-cdr`, `telecom-payments`, `telecom-location`, `telecom-security`, `telecom-network`, `telecom-device`, `telecom-profiles`, `telecom-complaints`, `telecom-usage`, `telecom-sdr`, `telecom-behavior`

---

## Phase 3: Processing Layer (Spark on WSL2)

### Real-time Stream Processor ([stream_processor.py](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/stream_processor.py))
**Input**: Kafka topics → **Output**: ClickHouse `fraud_alerts`

**Fraud Rules**:
- CDR: Duration > 15 min
- Payments: Amount > $100
- Security: Severity > 3
- Location: Roaming status
- Network: Packet loss > 1%

### Batch Processor ([batch_processor.py](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/batch_processor.py))
**Input**: HDFS Parquet files → **Output**: ClickHouse [daily_stats](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/batch_processor.py#56-185)

**Features**:
- Data Quality: Null checks, deduplication, range validation
- Multi-source aggregation: CDR + Payments + Location + Complaints
- Customer-level daily metrics

---

## Phase 4: Data Ingestion

### Kafka Connect HDFS Sink
**Replaces** manual Spark batch ingestion

**Configuration** ([connectors/hdfs-sink.json](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/connectors/hdfs-sink.json)):
- Flush size: 1000 records
- Rotation: 5 minutes or 1000 records
- Partitioning: Daily (`year=YYYY/month=MM/day=dd`)
- Location: `hdfs://namenode:9000/topics/{topic}/`

**Deployment**:
```powershell
.\scripts\deploy_connector.ps1
```

---

## Phase 5: Orchestration (Airflow)

### DAG ([dags/fraud_detection_dag.py](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/dags/fraud_detection_dag.py))
**Schedule**: Daily @ midnight

**Tasks**:
1. [process_daily_stats](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/batch_processor.py#56-185): Run [batch_processor.py](file:///D:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/batch_processor.py) via BashOperator
2. *(Future)*: Data quality checks, alerting

---

## Phase 6: Visualization (Grafana)

### Dashboard Panels
- **Real-time Alerts**: Time-series of fraud events
- **Daily Stats**: Bar chart of customer metrics
- **Top Fraudsters**: Table of high-risk customers

---

## Deployment Workflow

### Initial Setup
```powershell
# Clean old infrastructure
docker-compose down -v

# Start modernized stack
.\startup.ps1

# Verify
docker ps  # Should show 10+ containers
```

### Daily Operations
**Real-time Pipeline** (WSL):
```bash
bash run_spark.sh  # Producer + Spark + Dashboard
```

**Connector Status**:
```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/hdfs-sink-telecom/status"
```

---

## Technology Stack

| Layer | Technology | Reason |
|-------|------------|--------|
| **Streaming Bus** | Kafka (KRaft) | Simplified ops, no Zookeeper |
| **Data Lake** | HDFS (3 nodes) | Cost-effective, replication |
| **Ingestion** | Kafka Connect | Declarative, low overhead |
| **Processing** | Spark (WSL2) | Unified batch + streaming |
| **Serving** | ClickHouse | Fast analytics |
| **Orchestration** | Airflow | Scheduling, monitoring |
| **Viz** | Grafana | Open-source, ClickHouse plugin |

---

## Key Design Decisions

1. **KRaft over Zookeeper**: Reduces operational complexity, faster cluster formation
2. **Kafka Connect over Spark Ingest**: No manual job scheduling, automatic offset management
3. **Multi-node HDFS**: Production-grade data redundancy
4. **Spark on WSL2**: Lower resource usage than Docker, easier development
5. **Unified Alert Schema**: Single table for all fraud types enables cross-source correlation
