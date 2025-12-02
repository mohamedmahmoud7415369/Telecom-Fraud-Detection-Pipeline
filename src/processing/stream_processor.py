from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094')

def get_spark_session():
    return SparkSession.builder \
        .appName("TelecomFraudDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def process_batch(batch_df, batch_id):
    """
    Write fraud alerts to:
    1. ClickHouse (for Visualization)
    2. Kafka 'telecom-fraud-actions' (for Immediate Action)
    """
    if batch_df.count() > 0:
        # Cache the batch to avoid re-computing for double write
        batch_df.persist()
        
        try:
            # --- 1. Write to ClickHouse ---
            rows = batch_df.collect()
            import requests
            
            clickhouse_url = "http://localhost:8123/"
            auth = ('admin', 'admin123')
            
            print(f"[Batch {batch_id}] Processing {len(rows)} alerts...")
            
            for row in rows:
                ts_str = row.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                query = f"""
                INSERT INTO telecom_fraud.fraud_alerts 
                (call_id, caller_number, receiver_number, duration_min, timestamp, alert_type)
                VALUES 
                ('{row.call_id}', '{row.caller_number}', '{row.receiver_number}', 
                 {row.duration_min}, '{ts_str}', '{row.alert_type}')
                """
                try:
                    requests.post(clickhouse_url, auth=auth, data=query)
                except Exception as e:
                    print(f"Error writing to ClickHouse: {e}")

            # --- 2. Write to Kafka (Action Topic) ---
            # Convert columns to JSON 'value' column
            kafka_df = batch_df.selectExpr("to_json(struct(*)) AS value")
            
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", "telecom-fraud-actions") \
                .save()
                
            print(f"[Batch {batch_id}] Sent alerts to Kafka action topic")
            
        except Exception as e:
            print(f"[Batch {batch_id}] Error in batch processing: {e}")
        finally:
            batch_df.unpersist()

def process_stream():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # --- Rule 1: High Monthly Duration (CDR) ---
    # Actual CSV columns: customer_id,monthly_call_count,monthly_call_duration,international_call_duration,call_drop_count,sms_usage_per_month
    schema_cdr = StructType([
        StructField("customer_id", StringType()),
        StructField("monthly_call_count", IntegerType()),
        StructField("monthly_call_duration", FloatType()),
        StructField("international_call_duration", FloatType()),
        StructField("call_drop_count", IntegerType()),
        StructField("sms_usage_per_month", IntegerType())
    ])
    
    # Read stream
    df_cdr = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "telecom-cdr") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    parsed_cdr = df_cdr.select(from_json(col("value").cast("string"), schema_cdr).alias("data")).select("data.*")

    # --- 1. SIM Box Fraud (Gateway Bypass) ---
    # Pattern: High volume of outgoing calls (duration & count)
    fraud_simbox = parsed_cdr.filter(
        (col("monthly_call_duration") > 1000) & 
        (col("monthly_call_count") > 500)
    ).withColumn("alert_type", lit("SIM Box / Gateway Bypass")) \
     .withColumn("timestamp", current_timestamp()) \
     .withColumn("call_id", col("customer_id")) \
     .withColumn("caller_number", col("customer_id")) \
     .withColumn("receiver_number", lit("N/A")) \
     .withColumn("duration_min", col("monthly_call_duration")) \
     .select("call_id", "caller_number", "receiver_number", "duration_min", "timestamp", "alert_type")

    query_simbox = fraud_simbox.writeStream.foreachBatch(process_batch).start()

    # --- 2. Wangiri Fraud (One Ring Scam) ---
    # Pattern: Many calls with very short average duration (missed calls)
    fraud_wangiri = parsed_cdr.filter(
        (col("monthly_call_count") > 100) & 
        ((col("monthly_call_duration") / col("monthly_call_count")) < 1)
    ).withColumn("alert_type", lit("Wangiri (One Ring Scam)")) \
     .withColumn("timestamp", current_timestamp()) \
     .withColumn("call_id", col("customer_id")) \
     .withColumn("caller_number", col("customer_id")) \
     .withColumn("receiver_number", lit("N/A")) \
     .withColumn("duration_min", col("monthly_call_duration")) \
     .select("call_id", "caller_number", "receiver_number", "duration_min", "timestamp", "alert_type")

    query_wangiri = fraud_wangiri.writeStream.foreachBatch(process_batch).start()

    # --- 3. IRSF (International Revenue Share Fraud) ---
    # Pattern: Long duration international calls
    fraud_irsf = parsed_cdr.filter(
        col("international_call_duration") > 60
    ).withColumn("alert_type", lit("IRSF (High Int. Duration)")) \
     .withColumn("timestamp", current_timestamp()) \
     .withColumn("call_id", col("customer_id")) \
     .withColumn("caller_number", col("customer_id")) \
     .withColumn("receiver_number", lit("N/A")) \
     .withColumn("duration_min", col("international_call_duration")) \
     .select("call_id", "caller_number", "receiver_number", "duration_min", "timestamp", "alert_type")

    query_irsf = fraud_irsf.writeStream.foreachBatch(process_batch).start()

    # --- Rule 2: Payment Fraud Rules ---
    # Actual CSV columns: customer_id,monthly_spending,credit_score,payment_method,avg_payment_delay,payment_behavior_index,credit_limit
    schema_payment = StructType([
        StructField("customer_id", StringType()),
        StructField("monthly_spending", FloatType()),
        StructField("credit_score", IntegerType()),
        StructField("payment_method", StringType()),
        StructField("avg_payment_delay", IntegerType()),
        StructField("payment_behavior_index", FloatType()),
        StructField("credit_limit", FloatType())
    ])
    
    df_pay = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "telecom-payments") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
        
    parsed_pay = df_pay.select(from_json(col("value").cast("string"), schema_payment).alias("data")).select("data.*")
    
    # --- 4. Subscription Fraud ---
    # Pattern: High spending + Bad Credit + Late Payments
    fraud_subscription = parsed_pay.filter(
        (col("monthly_spending") > 400) & 
        (col("credit_score") < 550) & 
        (col("avg_payment_delay") > 5)
    ).withColumn("alert_type", lit("Subscription Fraud (Bad Debt)")) \
     .withColumn("timestamp", current_timestamp()) \
     .withColumn("call_id", col("customer_id")) \
     .withColumn("caller_number", col("customer_id")) \
     .withColumn("receiver_number", lit("N/A")) \
     .withColumn("duration_min", col("monthly_spending")) \
     .select("call_id", "caller_number", "receiver_number", "duration_min", "timestamp", "alert_type")

    query_subscription = fraud_subscription.writeStream.foreachBatch(process_batch).start()

    # --- 5. Credit Limit Abuse ---
    # Pattern: Spending exceeds credit limit
    fraud_credit = parsed_pay.filter(
        col("monthly_spending") > col("credit_limit")
    ).withColumn("alert_type", lit("Credit Limit Abuse")) \
     .withColumn("timestamp", current_timestamp()) \
     .withColumn("call_id", col("customer_id")) \
     .withColumn("caller_number", col("customer_id")) \
     .withColumn("receiver_number", lit("N/A")) \
     .withColumn("duration_min", col("monthly_spending")) \
     .select("call_id", "caller_number", "receiver_number", "duration_min", "timestamp", "alert_type")

    query_credit = fraud_credit.writeStream.foreachBatch(process_batch).start()

    # Await all
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
