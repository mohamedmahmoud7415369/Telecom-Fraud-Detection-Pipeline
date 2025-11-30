#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p logs

# Try to activate venv if it exists, otherwise use system python
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Using virtual environment"
elif [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "Using virtual environment (.venv)"
else
    echo "No venv found, using system Python"
    # Check if python3 exists, otherwise use python
    if command -v python3 &> /dev/null; then
        alias python=python3
    fi
fi

echo "-----------------------------------------------------"
echo "   Telecom Fraud Detection Pipeline - Unified Runner"
echo "-----------------------------------------------------"

# 1. Start Producer (Background)
echo "[1/3] Starting Data Producer..."
python src/source/producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!
echo "      Producer running (PID: $PRODUCER_PID). Logs: logs/producer.log"

# 2. Start Spark Job (Background)
echo "[2/3] Starting Spark Streaming Job..."
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    src/processing/stream_processor.py > logs/spark.log 2>&1 &
SPARK_PID=$!
echo "      Spark Job running (PID: $SPARK_PID). Logs: logs/spark.log"

# 3. Start Verification (Foreground)
echo "[3/3] Starting Alert Verification Dashboard..."
echo "      (Press Ctrl+C to stop everything)"
echo "-----------------------------------------------------"

# Function to kill background processes on exit
cleanup() {
    echo ""
    echo "Stopping pipeline..."
    kill $PRODUCER_PID 2>/dev/null
    kill $SPARK_PID 2>/dev/null
    echo "Done."
    exit
}

# Trap Ctrl+C
trap cleanup SIGINT

# Run Verification (replaced with log tailing to avoid connectivity issues)
# python src/processing/verify_alerts.py
echo "Pipeline running. Tailing Spark logs..."
tail -f logs/spark.log
