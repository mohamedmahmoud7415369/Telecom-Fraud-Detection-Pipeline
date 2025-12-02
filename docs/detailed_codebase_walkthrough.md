# Telecom Fraud Detection Pipeline - Deep Dive Documentation

This document provides a comprehensive, line-by-line explanation of the Telecom Fraud Detection Pipeline. It covers data sources, fraud detection logic, and the technical implementation of every component.

---

## 1. Data Sources (Detailed Schema)

The pipeline processes two main data streams simulated from CSV files.

### 📞 Call Detail Records (CDR)
**Source:** `cdr_data.csv` → **Kafka Topic:** `telecom-cdr`
**Description:** Records of every call made by subscribers.

| Field Name | Data Type | Description | Example Value |
|:---|:---|:---|:---|
| `customer_id` | String | Unique ID of the subscriber making the call. | `1000000001` |
| `monthly_call_count` | Integer | Total number of calls made in the current billing month. | `120` |
| `monthly_call_duration` | Float | Total minutes spent on calls this month. | `350.5` |
| `international_call_duration` | Float | Minutes spent on international calls (High risk). | `45.0` |
| `call_drop_count` | Integer | Number of calls that failed/dropped (Quality metric). | `2` |
| `sms_usage_per_month` | Integer | Total SMS messages sent. | `50` |

### 💳 Payment Transactions
**Source:** `payment_transactions.csv` → **Kafka Topic:** `telecom-payments`
**Description:** Financial transactions and credit standing of subscribers.

| Field Name | Data Type | Description | Example Value |
|:---|:---|:---|:---|
| `customer_id` | String | Unique ID of the subscriber. | `1000000001` |
| `monthly_spending` | Float | Total amount billed or spent this month (USD). | `85.00` |
| `credit_score` | Integer | Creditworthiness score (300-850). Lower is riskier. | `720` |
| `payment_method` | String | How they pay (Credit Card, Direct Debit, etc.). | `Credit Card` |
| `avg_payment_delay` | Integer | Average days past due date for payments. | `0` |
| `payment_behavior_index` | Float | Internal score of payment reliability (0.0 - 10.0). | `8.5` |
| `credit_limit` | Float | Maximum allowed spending limit. | `500.00` |

---

## 2. Fraud Detection Rules (Logic & Rationale)

We apply 5 specific rules based on real-world telecom fraud patterns (Egypt/USA standards).

### 🚨 1. SIM Box / Gateway Bypass
**Concept:** Fraudsters use a device (SIM Box) with hundreds of consumer SIM cards to route international calls as local calls, bypassing interconnect fees.
**Detection Logic:**
- **Condition:** `monthly_call_duration > 1000` AND `monthly_call_count > 500`
- **Why?** No normal human makes 16+ calls a day totaling 16+ hours a month consistently. This high-volume, high-frequency pattern is a signature of automated gateways.

### 🚨 2. Wangiri (One Ring Scam)
**Concept:** Scammers call thousands of numbers and hang up after one ring, hoping victims call back premium-rate numbers.
**Detection Logic:**
- **Condition:** `monthly_call_count > 100` AND [(monthly_call_duration / monthly_call_count) < 1](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/source/producer.py#64-72)
- **Why?** A high number of calls with an average duration of less than 1 minute indicates "missed call" spamming bots.

### 🚨 3. IRSF (International Revenue Share Fraud)
**Concept:** Fraudsters hack accounts or use stolen SIMs to make long calls to expensive international numbers they own, sharing the revenue.
**Detection Logic:**
- **Condition:** `international_call_duration > 60`
- **Why?** Legitimate users rarely make hour-long direct international calls (they use WhatsApp/Zoom). Sudden spikes in international duration are highly suspicious.

### 🚨 4. Subscription Fraud (Bad Debt)
**Concept:** A user signs up with no intention of paying, maxing out the line immediately.
**Detection Logic:**
- **Condition:** `monthly_spending > 400` AND `credit_score < 550` AND `avg_payment_delay > 5`
- **Why?** The combination of high spending, poor credit history, and late payments is a strong predictor of "bust-out" fraud (never paying the bill).

### 🚨 5. Credit Limit Abuse
**Concept:** Account takeover or system bypass where spending exceeds the assigned limit.
**Detection Logic:**
- **Condition:** `monthly_spending > credit_limit`
- **Why?** This is a hard rule. Spending should never technically exceed the limit; if it does, it implies a security breach or system failure.

---

## 3. Codebase Deep Dive (Line-by-Line)

### 📄 [src/source/producer.py](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/source/producer.py)
**Purpose:** Simulates the "Source System" by reading CSV files and streaming them to Kafka.

```python
30: def create_producer():
31:     try:
32:         producer = KafkaProducer(
33:             bootstrap_servers=KAFKA_BROKER,
34:             value_serializer=lambda v: json.dumps(v).encode('utf-8')
35:         )
```
- **Lines 32-35:** Initializes the Kafka Producer.
- **`bootstrap_servers`**: Tells it where the Kafka cluster is (`localhost:9092`).
- **`value_serializer`**: Converts Python dictionaries (our data) into JSON strings, then encodes them to bytes (UTF-8) so Kafka can store them.

```python
52:         for chunk in pd.read_csv(file_path, chunksize=1000):
53:             for _, row in chunk.iterrows():
54:                 message = row.to_dict()
55:                 producer.send(topic, value=message)
57:             producer.flush()
58:             time.sleep(0.1)
```
- **Line 52:** Reads the CSV file in chunks of 1000 rows. This is crucial for memory efficiency; we don't want to load a 10GB file entirely into RAM.
- **Line 55:** Sends a single record to the specified Kafka topic.
- **Line 58:** `time.sleep(0.1)` adds a 100ms delay after every 1000 records. This simulates a "stream" of data rather than an instant dump, allowing us to see the processing happen in real-time.

---

### 📄 [src/processing/stream_processor.py](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/stream_processor.py)
**Purpose:** The "Brain" of the system. Reads from Kafka, finds fraud, and alerts.

```python
10: def get_spark_session():
11:     return SparkSession.builder \
12:         .appName("TelecomFraudDetector") \
13:         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
14:         .getOrCreate()
```
- **Line 13:** Downloads the specific JAR file needed for Spark to talk to Kafka. We specify version `3.5.0` (Scala 2.12) to avoid compatibility errors we faced earlier.

```python
68:     df_cdr = spark.readStream \
69:         .format("kafka") \
70:         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
71:         .option("subscribe", "telecom-cdr") \
72:         .option("startingOffsets", "earliest") \
73:         .option("maxOffsetsPerTrigger", 1000) \
74:         .load()
```
- **Line 68:** Starts a streaming DataFrame.
- **Line 72:** `startingOffsets: earliest` means "read from the beginning of the topic" (don't miss old data).
- **Line 73:** `maxOffsetsPerTrigger: 1000` is a **critical performance tuning**. It tells Spark: "Only process 1000 records at a time." Without this, Spark tried to read millions of records at once and crashed with `OutOfMemoryError`.

```python
80:     fraud_simbox = parsed_cdr.filter(
81:         (col("monthly_call_duration") > 1000) & 
82:         (col("monthly_call_count") > 500)
83:     ).withColumn("alert_type", lit("SIM Box / Gateway Bypass"))
```
- **Lines 80-83:** The actual fraud logic. `filter` keeps only rows matching our SIM Box rule.
- **Line 83:** Adds a new column `alert_type` with the static text "SIM Box..." so we know *why* this was flagged.

```python
16: def process_batch(batch_df, batch_id):
17:     """
18:     Write fraud alerts to:
19:     1. ClickHouse (for Visualization)
20:     2. Kafka 'telecom-fraud-actions' (for Immediate Action)
21:     """
22:     if batch_df.count() > 0:
23:         batch_df.persist()  # Cache to avoid re-computation
```
- **Lines 16-23:** Refactored function now writes to **two destinations** instead of one.
- **Line 23:** `persist()` caches the DataFrame in memory. Critical optimization—without this, Spark would re-execute all the fraud detection logic twice (once for ClickHouse, once for Kafka), doubling the processing time.

```python
32:             ts_str = row.timestamp.strftime('%Y-%m-%d %H:%M:%S')
...
44:                 requests.post(clickhouse_url, auth=auth, data=query)
```
- **Line 32:** Formats the timestamp. ClickHouse is strict and rejected timestamps like `02:57:38.765`. We strip the microseconds (`.765`) to make it `02:57:38`.
- **Line 44:** Uses a standard HTTP POST request to insert data into ClickHouse. This replaced the complex JDBC driver approach which was causing `ClassNotFoundException` errors.

```python
50:             kafka_df = batch_df.selectExpr("to_json(struct(*)) AS value")
51:             
52:             kafka_df.write \
53:                 .format("kafka") \
54:                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
55:                 .option("topic", "telecom-fraud-actions") \
56:                 .save()
```
- **Line 50:** Converts the entire row into a JSON string and stores it in a column named `value` (Kafka's required format).
- **Lines 52-56:** Uses Spark's native Kafka connector to write alerts to the `telecom-fraud-actions` topic. This is **non-blocking** and completes in ~5ms, preventing the Speed Layer from being slowed down.

---

### 📄 [src/processing/action_consumer.py](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/src/processing/action_consumer.py)
**Purpose:** Lightweight consumer that executes fraud prevention actions (blocking users, sending alerts).

```python
24:     consumer = KafkaConsumer(
25:         TOPIC_NAME,
26:         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
27:         auto_offset_reset='latest',
28:         group_id='fraud-action-group',
29:         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
30:     )
```
- **Line 27:** `auto_offset_reset='latest'` means "only process new alerts" (don't replay old ones from when the consumer was offline).
- **Line 28:** `group_id='fraud-action-group'` enables **consumer groups**. If you run 3 instances of this script, Kafka automatically distributes the workload among them (horizontal scaling).
- **Line 29:** Deserializes the JSON string back into a Python dictionary.

```python
15: def block_user(user_id, reason):
16:     time.sleep(0.05)  # Simulate API latency
17:     print(f"[ACTION] 🚫 BLOCKED User {user_id} | Reason: {reason}")
```
- **Line 16:** Simulates a 50ms HTTP request to the HLR/HSS (Home Location Register) that would disable the SIM card.
- **Production Extension:** Replace this with:
  ```python
  requests.post("https://hlr.telecom.com/block", json={"msisdn": user_id})
  ```

---

### 📄 [docker-compose.yml](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/docker-compose.yml)
**Purpose:** Defines the infrastructure (Servers) as code.

```yaml
5:   kafka-1:
6:     image: confluentinc/cp-kafka:7.4.0
...
16:       KAFKA_PROCESS_ROLES: 'broker,controller'
```
- **Line 3:** We use **KRaft mode** (Kafka Raft), which is the modern way to run Kafka without Zookeeper. It simplifies the setup.
- **Line 16:** This node acts as both a `broker` (stores data) and a `controller` (manages the cluster).

```yaml
83:   clickhouse:
84:     image: clickhouse/clickhouse-server
...
90:       CLICKHOUSE_DB: telecom_fraud
```
- **Line 83:** Runs the ClickHouse database server.
- **Line 90:** Automatically creates the `telecom_fraud` database when the container starts.

---

### 📄 [scripts/run_spark.sh](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/scripts/run_spark.sh)
**Purpose:** The "One-Click" script to run everything.

```bash
7: if [ -d "venv" ]; then
8:     source venv/bin/activate
```
- **Lines 7-8:** Automatically finds and activates your Python Virtual Environment. This ensures we use the correct libraries (`pyspark`, `requests`) installed in `venv`, avoiding system conflicts.

```bash
27: python src/source/producer.py > logs/producer.log 2>&1 &
```
- **`>`**: Redirects standard output (print statements) to a file.
- **`2>&1`**: Redirects errors to the same file.
- **`&`**: Runs the command in the **background**, so the terminal doesn't freeze.

```bash
55: trap cleanup SIGINT
```
- **Line 55:** A safety feature. If you press `Ctrl+C` to stop the script, this `trap` catches the signal and runs the [cleanup()](file:///d:/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline/scripts/run_spark.sh#45-53) function (Line 45) to kill the background Producer and Spark processes, so they don't keep running as "zombies."
