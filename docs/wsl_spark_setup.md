# Setting up Spark on WSL2

Since you chose to run Spark natively in WSL2 (Option 2), follow these steps to set up your environment.

## 1. Install Java (OpenJDK 11)
Spark requires Java. Run these commands in your WSL terminal:

```bash
sudo apt-get update
sudo apt-get install openjdk-11-jdk -y
```

Verify installation:
```bash
java -version
```

## 2. Install Apache Spark
Download and extract Spark (we use 3.5.0 to match our plan).

```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

# Extract it
tar xvf spark-3.5.0-bin-hadoop3.tgz

# Move to a clean directory
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
```

## 3. Configure Environment Variables
Add Spark to your path. Edit your `.bashrc`:

```bash
nano ~/.bashrc
```

Add these lines to the bottom:
```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
```
```
## This error happens because newer versions of Ubuntu protect the system Python.

## Solution: Use a Virtual Environment.

## I have updated the guide (docs/wsl_spark_setup.md) with the fix. Please run these commands in your WSL terminal:

## Install venv tool:
bash
sudo apt install python3-venv -y

## Create the environment (make sure you are in the project folder):

```bash
cd /mnt/d/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline python3 -m venv venv
```
```

## Activate it:

```bash
source venv/bin/activate
```
```
## Now install the packages:

```bash
pip install pyspark kafka-python clickhouse-driver panda
```
```