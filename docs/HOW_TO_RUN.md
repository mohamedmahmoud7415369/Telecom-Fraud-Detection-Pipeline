# How to Run the Telecom Fraud Detection System

## Prerequisites
- Docker Desktop running
- WSL2 installed (for Spark)
- Python environment with dependencies installed

---

## Step 1: Start Infrastructure (Windows PowerShell)

```powershell
cd d:\ITI-Data_Engineer\Projects\Telecom-Fraud-Detection-Pipeline

# Start all Docker containers
docker-compose up -d

# Wait 30 seconds for services to initialize
Start-Sleep -Seconds 30

# Initialize Kafka topics (including telecom-fraud-actions)
python src/source/init_kafka.py

# Initialize ClickHouse tables
python src/processing/init_clickhouse.py
```

**Verify containers are running:**
```powershell
docker ps
```
You should see: `kafka-1`, `kafka-2`, `kafka-3`, `clickhouse`, `grafana`

---

## Step 2: Access Grafana Dashboard (Browser)

1. Open: **http://localhost:3000**
2. Login:
   - Username: `admin`
   - Password: `admin`
3. Navigate to: **Dashboards** → **Telecom Fraud Detection - Real-time Dashboard**
4. Keep this tab open to watch real-time updates

---

## Step 3: Start Speed Layer (WSL Terminal)

Open a WSL terminal and run:

```bash
cd /mnt/d/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline
bash scripts/run_spark.sh
```

**What this does:**
- Starts the **Producer** (reads CSV files, sends to Kafka)
- Starts **Spark Streaming** (detects fraud, writes to ClickHouse + Action Topic)

**You'll see output like:**
```
[Producer] Sent 1000 CDR records...
[Batch 0] Processing 15 alerts...
[Batch 0] Sent alerts to Kafka action topic
```

---

## Step 4: Start Action Consumer (Windows PowerShell - New Terminal)

Open a **new** PowerShell terminal:

```powershell
cd d:\ITI-Data_Engineer\Projects\Telecom-Fraud-Detection-Pipeline
python src/processing/action_consumer.py
```

**You'll see blocking actions:**
```
Starting Action Consumer on topic: telecom-fraud-actions
Waiting for fraud alerts...
[ACTION] 🚫 BLOCKED User CUST001 | Reason: SIM Box / Gateway Bypass
[ACTION] 🚫 BLOCKED User CUST045 | Reason: Credit Limit Abuse
```

---

## Step 5: Watch the System Work

**In Grafana (Browser):**
- Time series chart updates every 5 seconds
- Gauge shows total alerts
- Pie chart shows fraud type distribution
- Tables show flagged users and recent alerts

**In Action Consumer Terminal:**
- Live blocking messages appear as fraud is detected

**In Spark Terminal (WSL):**
- Batch processing logs show data flow

---

## Stopping the System

**Stop Spark (in WSL terminal):**
Press `Ctrl+C`

**Stop Action Consumer (in PowerShell):**
Press `Ctrl+C`

**Stop Infrastructure (in PowerShell):**
```powershell
docker-compose down
```

---

## Troubleshooting

### Grafana shows "No data"
- Ensure Spark is running in WSL
- Check ClickHouse has data:
  ```powershell
  docker exec -it clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM telecom_fraud.fraud_alerts"
  ```

### Action Consumer not receiving alerts
- Verify Kafka topic exists:
  ```powershell
  docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list | Select-String "fraud-actions"
  ```

### Spark fails to start
- Check Java is installed: `java -version` (in WSL)
- Verify Python dependencies: `pip list | grep pyspark`

---

## Quick Reference

| Component | Location | Command |
|-----------|----------|---------|
| **Infrastructure** | Windows PowerShell | `docker-compose up -d` |
| **Grafana Dashboard** | Browser | http://localhost:3000 |
| **Speed Layer** | WSL | `bash scripts/run_spark.sh` |
| **Action Consumer** | Windows PowerShell | `python src/processing/action_consumer.py` |
| **ClickHouse** | Docker | Port 8123 (HTTP), 9001 (Native) |
| **Kafka** | Docker | Ports 9092, 9093, 9094 |
