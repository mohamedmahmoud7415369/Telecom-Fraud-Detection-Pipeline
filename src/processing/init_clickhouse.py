import clickhouse_driver

# Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9001 

def create_tables():
    try:
        client = clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user='admin', password='admin123')
        
        # 1. Create Database
        client.execute('CREATE DATABASE IF NOT EXISTS telecom_fraud')
        print("Database 'telecom_fraud' checked/created.")

        # 2. Create Fraud Alerts Table (Speed Layer)
        # Stores real-time alerts for calls > 60 mins
        client.execute('''
            CREATE TABLE IF NOT EXISTS telecom_fraud.fraud_alerts (
                call_id String,
                caller_number String,
                receiver_number String,
                duration_min Float32,
                timestamp DateTime,
                alert_type String
            ) ENGINE = MergeTree()
            ORDER BY timestamp
        ''')
        print("Table 'fraud_alerts' checked/created.")

        # 3. Create Daily Stats Table (Batch Layer)
        # Stores aggregated daily usage stats
        client.execute('''
            CREATE TABLE IF NOT EXISTS telecom_fraud.daily_stats (
                report_date Date,
                customer_id String,
                total_calls UInt32,
                total_duration Float32,
                avg_duration Float32,
                total_spend Float32,
                avg_spend Float32,
                unique_locations UInt32,
                avg_signal_strength Float32,
                complaint_count UInt32,
                fraud_cases UInt32
            ) ENGINE = MergeTree()
            ORDER BY (report_date, customer_id)
        ''')
        print("Table 'daily_stats' checked/created.")

    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")

if __name__ == "__main__":
    create_tables()
