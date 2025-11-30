import clickhouse_driver
import time
import os

# Configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9001 

def verify_alerts():
    try:
        client = clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user='admin', password='admin123')
        
        print("--- Checking ClickHouse for Fraud Alerts ---")
        
        while True:
            # Get total count
            count = client.execute('SELECT count(*) FROM telecom_fraud.fraud_alerts')[0][0]
            print(f"Total Alerts: {count}")
            
            if count > 0:
                # Get latest 5 alerts
                alerts = client.execute('''
                    SELECT alert_type, caller_number, duration_min, timestamp 
                    FROM telecom_fraud.fraud_alerts 
                    ORDER BY timestamp DESC 
                    LIMIT 5
                ''')
                
                print("\nLatest 5 Alerts:")
                print(f"{'Alert Type':<25} | {'Caller':<15} | {'Value':<10} | {'Time'}")
                print("-" * 70)
                for row in alerts:
                    print(f"{row[0]:<25} | {row[1]:<15} | {row[2]:<10} | {row[3]}")
            
            print("\nWaiting for more data... (Press Ctrl+C to stop)")
            time.sleep(5)
            # Clear screen for dashboard effect (optional, works in some terminals)
            # os.system('cls' if os.name == 'nt' else 'clear')

    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")

if __name__ == "__main__":
    verify_alerts()
