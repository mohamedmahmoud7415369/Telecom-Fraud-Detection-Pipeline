# Telecom Fraud Detection Pipeline - Task List

- [x] **Phase 1: Infrastructure Setup**
    - [x] Define Architecture (Lambda, ClickHouse, WSL2 Spark).
    - [x] Create `docker-compose.yml` (Kafka x3, HDFS, ClickHouse, Airflow, Grafana).
    - [x] Verify Docker containers are running.

- [x] **Phase 2: Source System**
    - [x] Create `producer.py` to simulate stream.
    - [x] Create `init_kafka.py` to configure topics.
    - [ ] Verify data arriving in Kafka (Run producer).

- [ ] **Phase 3: Spark Setup (WSL2)**
    - [ ] Install Java 11 & Spark 3.5.0 in WSL2.
    - [ ] Install Python dependencies (`pyspark`, `kafka-python`, `clickhouse-driver`).

- [ ] **Phase 4: Real-time Processing (Speed Layer)**
    - [ ] Develop `stream_processor.py` (Read Kafka -> Write ClickHouse).
    - [ ] Test real-time fraud detection logic.

- [ ] **Phase 5: Batch Processing (Batch Layer)**
    - [ ] Develop `batch_ingest.py` (Read Kafka -> Write HDFS).
    - [ ] Develop `batch_processor.py` (Read HDFS -> Write ClickHouse).

- [ ] **Phase 6: Orchestration & Serving**
    - [ ] Create Airflow DAG to schedule batch job.
    - [ ] Build Grafana Dashboard connecting to ClickHouse.
