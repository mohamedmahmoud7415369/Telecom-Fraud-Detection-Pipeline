# Telecom Fraud Detection Pipeline - Technical Documentation

## 1. Data Sources

The pipeline ingests data from two primary CSV sources, simulated as real-time streams via Kafka.

### 📞 Call Detail Records (CDR)
**Topic:** `telecom-cdr`
**Source File:** `cdr_data.csv`

| Field Name | Type | Description |
|:---|:---|:---|
| `customer_id` | String | Unique identifier for the subscriber. |
| `monthly_call_count` | Integer | Total number of calls made in the month. |
| `monthly_call_duration` | Float | Total duration of all calls in minutes. |
| `international_call_duration` | Float | Duration of international calls in minutes. |
| `call_drop_count` | Integer | Number of dropped calls (quality metric). |
| `sms_usage_per_month` | Integer | Total number of SMS sent. |

### 💳 Payment Transactions
**Topic:** `telecom-payments`
**Source File:** `payment_transactions.csv`

| Field Name | Type | Description |
|:---|:---|:---|
| `customer_id` | String | Unique identifier for the subscriber. |
| `monthly_spending` | Float | Total amount billed/spent in the month (USD). |
| `credit_score` | Integer | Customer's credit score (300-850). |
| `payment_method` | String | Method of payment (Credit Card, Direct Debit, etc.). |
| `avg_payment_delay` | Integer | Average days late for payments. |
| `payment_behavior_index` | Float | Calculated score of payment reliability. |
| `credit_limit` | Float | Maximum allowed spending limit. |

---

## 2. Fraud Detection Rules

The system applies 5 real-time rules based on Egypt/USA telecom fraud standards.

### 1. SIM Box / Gateway Bypass
**Description:** Devices that route international calls through local SIMs to bypass interconnect fees.
**Logic:**
- **High Volume:** `monthly_call_duration > 1000` (Over 16 hours)
- **High Frequency:** `monthly_call_count > 500` (Over 16 calls/day)
**Why:** Normal users rarely sustain this volume; indicates automated routing.

### 2. Wangiri (One Ring Scam)
**Description:** Scammers call and hang up immediately to trick victims into calling back premium numbers.
**Logic:**
- **Many Calls:** `monthly_call_count > 100`
- **Short Duration:** `avg_call_duration < 1` minute (Total Duration / Total Count)
**Why:** High volume of extremely short calls is the signature of Wangiri bots.

### 3. IRSF (International Revenue Share Fraud)
**Description:** Long duration calls to high-cost international destinations to generate revenue for the fraudster.
**Logic:**
- **Long Int. Calls:** `international_call_duration > 60` minutes
**Why:** Most legitimate users use VoIP (WhatsApp/Zoom) for long international calls; direct carrier usage is suspicious.

### 4. Subscription Fraud (Bad Debt)
**Description:** Users with poor credit who sign up with no intention to pay.
**Logic:**
- **High Spend:** `monthly_spending > 400`
- **Bad Credit:** `credit_score < 550`
- **Late Payer:** `avg_payment_delay > 5` days
**Why:** High usage immediately combined with poor credit history indicates "bust-out" fraud.

### 5. Credit Limit Abuse
**Description:** Account takeover or abuse where spending exceeds the assigned limit.
**Logic:**
- **Over Limit:** `monthly_spending > credit_limit`
**Why:** Spending should never exceed the hard limit; indicates system bypass or hacking.

---

## 3. Codebase Deep Dive

### 📄 [src/source/producer.py](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/source/producer.py)
**Purpose:** Simulates real-time data ingestion by reading CSVs and sending them to Kafka.

```python
# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # Serialize dict to JSON bytes
)

# Streaming Logic
for chunk in pd.read_csv(file_path, chunksize=1000): # Read in batches of 1000 to save memory
    for _, row in chunk.iterrows():
        message = row.to_dict()
        producer.send(topic, value=message) # Send individual record
    
    producer.flush() # Ensure data is sent
    time.sleep(0.1) # Artificial delay to simulate real-time stream
```

### 📄 [src/processing/stream_processor.py](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/stream_processor.py)
**Purpose:** The core engine. Reads from Kafka, applies fraud rules using Spark Structured Streaming, and writes alerts to ClickHouse.

**1. Spark Session Setup:**
```python
SparkSession.builder \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") # Kafka Connector
```
*Note: We use Spark 3.5.0 connector to avoid compatibility issues.*

**2. Reading Stream:**
```python
df_cdr = spark.readStream \
    .format("kafka") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) # LIMIT BATCH SIZE to prevent OutOfMemory
    .load()
```
*Key Logic: `maxOffsetsPerTrigger` prevents the driver from crashing when processing millions of historical records.*

**3. Parsing Data:**
```python
# Convert JSON string (Kafka value) to Spark Struct (Columns)
parsed_cdr = df_cdr.select(from_json(col("value").cast("string"), schema_cdr).alias("data")).select("data.*")
```

**4. Applying Fraud Logic (Example: SIM Box):**
```python
fraud_simbox = parsed_cdr.filter(
    (col("monthly_call_duration") > 1000) & 
    (col("monthly_call_count") > 500)
).withColumn("alert_type", lit("SIM Box")) # Tag the alert
```

**5. Writing to ClickHouse (HTTP Interface):**
```python
def write_to_clickhouse(batch_df, batch_id):
    rows = batch_df.collect() # Bring data to driver
    for row in rows:
        # Format timestamp to remove microseconds (ClickHouse incompatibility)
        ts_str = row.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Send HTTP POST request to ClickHouse
        requests.post("http://localhost:8123/", data=f"INSERT INTO ... VALUES ...")
```
*Why HTTP?* The native JDBC driver caused Java ClassNotFound errors. HTTP is simpler and more reliable for Python.

### 📄 [scripts/run_spark.sh](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/scripts/run_spark.sh)
**Purpose:** Orchestrates the pipeline execution.

```bash
# 1. Activate Virtual Environment (Isolation)
source venv/bin/activate

# 2. Start Producer in Background (&)
python src/source/producer.py > logs/producer.log 2>&1 &

# 3. Start Spark Job in Background (&)
spark-submit ... src/processing/stream_processor.py > logs/spark.log 2>&1 &

# 4. Cleanup Function
# Kills background PIDs when you press Ctrl+C
trap cleanup SIGINT
```

### 📄 [src/processing/init_clickhouse.py](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/init_clickhouse.py)
**Purpose:** Initializes the database schema.

```python
# Create MergeTree table (Optimized for time-series data)
CREATE TABLE IF NOT EXISTS telecom_fraud.fraud_alerts (
    ...
) ENGINE = MergeTree()
ORDER BY timestamp # Primary key for sorting/indexing
```
