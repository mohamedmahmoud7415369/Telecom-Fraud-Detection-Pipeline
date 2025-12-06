#!/usr/bin/env python
# coding: utf-8

# In[12]:


get_ipython().system('hdfs dfs -ls /telecom_project/batch_layer')


# In[13]:


get_ipython().system('hdfs dfs -ls /telecom_project/stream_layer')


# In[1]:


import sys


get_ipython().system('{sys.executable} -m pip install pyspark findspark --break-system-packages')


import findspark
findspark.init()

from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Telecom Setup") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)
print("✅ الآن التثبيت حقيقي والـ Spark جاهز!")


# In[2]:


import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Telecom_QA") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()


print("✅ Spark is ready!")


def load_data_fast(layer, filename):
    path = f"hdfs://master:8020/telecom_project/{layer}/{filename}"


    return spark.read.csv(path, header=True, inferSchema=False)


df_complaints = load_data_fast("batch_layer", "complaints_feedback.csv")
df_behavior = load_data_fast("batch_layer", "customer_behavior.csv")
df_profiles = load_data_fast("batch_layer", "customer_profiles.csv")
df_device = load_data_fast("batch_layer", "device_information.csv")

df_cdr = load_data_fast("stream_layer", "cdr_data.csv")
df_location = load_data_fast("stream_layer", "location_updates.csv")
df_network = load_data_fast("stream_layer", "network_metrics.csv")
df_payments = load_data_fast("stream_layer", "payment_transactions.csv")
df_sdr = load_data_fast("stream_layer", "sdr_data.csv")
df_security = load_data_fast("stream_layer", "security_events.csv")
df_service = load_data_fast("stream_layer", "service_usage.csv")

print("\n🚀 Done! Spark now knows where the files are.")


df_cdr.show(5)


# In[4]:


def read_with_schema(layer, filename, ratio=0.05):
    path = f"hdfs://master:8020/telecom_project/{layer}/{filename}"
    return spark.read.csv(
        path,
        header=True,
        inferSchema=True,
        samplingRatio=ratio
    )

df_complaints = read_with_schema("batch_layer", "complaints_feedback.csv")
df_behavior   = read_with_schema("batch_layer", "customer_behavior.csv")
df_profiles   = read_with_schema("batch_layer", "customer_profiles.csv")
df_device     = read_with_schema("batch_layer", "device_information.csv")

df_cdr        = read_with_schema("stream_layer", "cdr_data.csv")
df_location   = read_with_schema("stream_layer", "location_updates.csv")
df_network    = read_with_schema("stream_layer", "network_metrics.csv")
df_payments   = read_with_schema("stream_layer", "payment_transactions.csv")
df_sdr        = read_with_schema("stream_layer", "sdr_data.csv")
df_security   = read_with_schema("stream_layer", "security_events.csv")
df_service    = read_with_schema("stream_layer", "service_usage.csv")


# In[5]:


all_dfs = {
    "complaints": df_complaints,
    "behavior": df_behavior,
    "profiles": df_profiles,
    "device": df_device,
    "cdr": df_cdr,
    "location": df_location,
    "network": df_network,
    "payments": df_payments,
    "sdr": df_sdr,
    "security": df_security,
    "service": df_service
}

for name, df in all_dfs.items():
    print(f"\n===== SCHEMA: {name} =====")
    df.printSchema()


# In[6]:


from pyspark.sql.functions import *  
import pyspark.sql.functions as F


# In[7]:


##complaints csv


# In[8]:


df_complaints.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df_complaints.columns]
).show()


# In[9]:


df_complaints.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:





# In[ ]:


##customer behavior csv


# In[10]:


df_behavior.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_behavior.columns]).show()


# In[9]:


df_behavior.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##customer_profiles


# In[11]:


df_profiles.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_profiles.columns]).show()


# In[12]:


df_profiles.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##device_information


# In[13]:


df_device.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_device.columns]).show()


# In[14]:


df_device.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##stream


# In[ ]:


##cdr_data


# In[17]:


df_cdr.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_cdr.columns]).show()


# In[16]:


df_cdr.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##location_updates


# In[18]:


df_location.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_location.columns]).show()


# In[19]:


df_location.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##network_metrics


# In[20]:


df_network.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_network.columns]).show()


# In[24]:


from pyspark.sql import functions as F

# عمل column hash لكل row
df_network_hash = df_network.withColumn(
    "row_hash",
    F.md5(F.concat_ws("||", *df_network.columns))
)

# count لكل hash
duplicates = df_network_hash.groupBy("row_hash").count().filter(F.col("count") > 1)

duplicates.show()


# In[ ]:


##payment_transactions


# In[26]:


df_payments.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_payments.columns]).show()


# In[27]:


df_payments.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##sdr_data


# In[28]:


df_sdr.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_sdr.columns]).show()


# In[29]:


df_sdr.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##security_events


# In[31]:


df_security.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_security.columns]).show()


# In[32]:


df_security.groupBy("customer_id").count().filter("count > 1").show()


# In[ ]:


##service_usage


# In[33]:


df_service.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_service.columns]).show()


# In[34]:


df_service.groupBy("customer_id").count().filter("count > 1").show()


# In[37]:


## data modeling


# In[11]:


## Dim_Customerimport findspark
findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Smart Schema Loading") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("✅ Spark is Ready!")


def read_with_schema(layer, filename, ratio=0.05):
    path = f"hdfs://master:8020/telecom_project/{layer}/{filename}"
    return spark.read.csv(
        path, 
        header=True, 
        inferSchema=True, 
        samplingRatio=ratio
    )

print("⏳ Loading Data with Sampling (5%)... Please wait...")


# --- Batch Layer ---
df_complaints = read_with_schema("batch_layer", "complaints_feedback.csv")
df_behavior   = read_with_schema("batch_layer", "customer_behavior.csv")
df_profiles   = read_with_schema("batch_layer", "customer_profiles.csv")
df_device     = read_with_schema("batch_layer", "device_information.csv")

# --- Stream Layer ---
df_cdr        = read_with_schema("stream_layer", "cdr_data.csv")
df_location   = read_with_schema("stream_layer", "location_updates.csv")
df_network    = read_with_schema("stream_layer", "network_metrics.csv")
df_payments   = read_with_schema("stream_layer", "payment_transactions.csv")
df_sdr        = read_with_schema("stream_layer", "sdr_data.csv")
df_security   = read_with_schema("stream_layer", "security_events.csv")
df_service    = read_with_schema("stream_layer", "service_usage.csv")

print("✅ All Files Loaded Successfully!\n")

# 4. عرض الـ Schema للتأكد (Loop عشان نشوفهم كلهم)
all_dfs = {
    "complaints": df_complaints,
    "behavior": df_behavior,
    "profiles": df_profiles,
    "device": df_device,
    "cdr": df_cdr,
    "location": df_location,
    "network": df_network,
    "payments": df_payments,
    "sdr": df_sdr,
    "security": df_security,
    "service": df_service
}

for name, df in all_dfs.items():
    print(f"\n===== SCHEMA: {name} =====")
    df.printSchema()


# In[ ]:


##1. Dim_Customer (دمج Profiles + Behavior)


# In[12]:


from pyspark.sql.functions import col

print("Building Dimension: Dim_Customer ...")

dim_customer_raw = df_profiles.join(df_behavior, on="customer_id", how="left")

final_dim_customer = dim_customer_raw.select(
    col("customer_id"),
    col("age"),
    col("gender"),
    col("region"),
    col("marital_status"),
    col("education_level"),
    col("employment_status"),
    col("income"),
    col("preferred_language"),

    col("monthly_online_purchases"),
    col("avg_cart_value"),
    col("most_purchased_category"),
    col("preferred_contact_channel"),
    col("offer_response_rate")
)

print(f" Dim_Customer Created. Rows: {final_dim_customer.count()}")
final_dim_customer.show(5)
final_dim_customer.printSchema()


# In[ ]:


## Dim_Device


# In[13]:


final_dim_device = df_device.select(
    col("customer_id"),
    col("device_type"),
    col("num_devices_registered"),
    col("device_age_months"),
    col("network_generation"),
    col("os_type"),
    col("uses_5g"),
    col("device_security_patch")
)

print(f"Dim_Device Created. Rows: {final_dim_device.count()}")
final_dim_device.show(5)
final_dim_device.printSchema()


# In[ ]:


## Dim_Location


# In[14]:


final_dim_location = df_location.select(
    col("tower_id"),
    col("city"),
    col("state"),
    col("zip_code"),
    col("latitude"),
    col("longitude"),
    col("urbanization_level")
).distinct()

print(f" Dim_Location Created. Unique Towers: {final_dim_location.count()}")
final_dim_location.show(5)
final_dim_location.printSchema()


# In[46]:


## Dim_date


# In[19]:


from pyspark.sql.functions import explode, sequence, to_date, year, month, dayofmonth, quarter, date_format, dayofweek, col


spark.sql("""
    SELECT explode(sequence(to_date('2023-01-01'), to_date('2025-12-31'), interval 1 day)) as date_id
""").createOrReplaceTempView("dates_temp")

final_dim_time = spark.sql("""
    SELECT 
        date_id,
        year(date_id) as year,
        month(date_id) as month,
        day(date_id) as day,
        quarter(date_id) as quarter,
        date_format(date_id, 'EEEE') as day_name,
        CASE WHEN dayofweek(date_id) IN (1, 7) THEN true ELSE false END as is_weekend
    FROM dates_temp
""")

print(f"Dim_Time Created. Days: {final_dim_time.count()}")
final_dim_time.show(5)
final_dim_time.printSchema()


# In[ ]:


## Call Detail Records  Fact_CDR


# In[20]:


fact_cdr = df_cdr.select(
    col("customer_id"),                
    col("monthly_call_count"),
    col("monthly_call_duration"),
    col("international_call_duration"),
    col("call_drop_count"),
    col("sms_usage_per_month")
)

print(f"Fact_CDR Created. Rows: {fact_cdr.count()}")
fact_cdr.show(5)
fact_cdr.printSchema()


# In[ ]:


## Fact_Payments


# In[21]:


fact_payments = df_payments.select(
    col("customer_id"),
    col("monthly_spending"),
    col("credit_score"),
    col("payment_method"),
    col("avg_payment_delay"),
    col("payment_behavior_index"),
    col("credit_limit")
)

print(f"Fact_Payments Created. Rows: {fact_payments.count()}")
fact_payments.show(5)
fact_payments.printSchema()


# In[52]:


## Fact_Complaints


# In[22]:


fact_complaints = df_complaints.select(
    col("customer_id"),
    col("customer_satisfaction_score"),
    col("customer_complaints"),
    col("technical_support_rating"),
    col("churn_risk_score"),
    col("has_contacted_support")
)

print(f"Fact_Complaints Created. Rows: {fact_complaints.count()}")
fact_complaints.show(5)
fact_payments.printSchema()


# In[ ]:


## Fact_Internet_Usage


# In[23]:


fact_internet = df_sdr.select(
    col("customer_id"),
    col("internet_usage_gb"),
    col("internet_speed_avg_mbps"),
    col("latency_ms"),
    col("peak_usage_hour"),
    col("weekend_data_ratio")
)

print(f"Fact_Internet_Usage Created. Rows: {fact_internet.count()}")
fact_internet.show(5)
fact_internet.printSchema()


# In[ ]:


## Fact_Network_Metrics


# In[24]:


fact_network = df_network.select(
    col("tower_id"),
    col("signal_strength_avg"),
    col("network_downtime_minutes"),
    col("network_congestion_level"),
    col("avg_signal_strength")
)

print(f"Fact_Network_Metrics Created. Rows: {fact_network.count()}")
fact_network.show(5)
fact_internet.printSchema()


# In[ ]:


## Fact_Security


# In[25]:


fact_security = df_security.select(
    col("customer_id"),
    col("num_failed_logins"),
    col("num_fraud_attempts"),
    col("encryption_type"),
    col("biometric_auth"),
    col("two_factor_auth")
)

print(f"Fact_Security Created. Rows: {fact_security.count()}")
fact_security.show(5)
fact_security.printSchema()


# In[26]:


def insert_safe(spark_df, table_name, batch_size=50000):
    print(f"🚀 بدء نقل جدول: {table_name} ...")
    try:

        client.execute(f'TRUNCATE TABLE IF EXISTS {table_name}')

        batch = []
        total_inserted = 0


        # toLocalIterator هو البطل هنا، بيجيب صف بصف
        for row in spark_df.toLocalIterator():
            batch.append(row)


            if len(batch) >= batch_size:
                client.execute(f'INSERT INTO {table_name} VALUES', batch)
                total_inserted += len(batch)
                print(f"   -> تم نقل {total_inserted} صف...")
                batch = [] # فضي السلة


        if batch:
            client.execute(f'INSERT INTO {table_name} VALUES', batch)
            total_inserted += len(batch)

        print(f"✅ تم الانتهاء من {table_name}: إجمالي {total_inserted} صف.\n")

    except Exception as e:
        print(f"❌ خطأ أثناء نقل {table_name}: {e}")


# In[28]:


from clickhouse_driver import Client

# ==========================================

# ==========================================
CLICKHOUSE_IP = '172.23.0.3' 
try:
    client = Client(host=CLICKHOUSE_IP, port=9000, user='admin', password='admin123', database='default')
    client.execute('SHOW TABLES')
    print(f"✅ تم إعادة الاتصال بـ ClickHouse بنجاح.")
except Exception as e:
    print(f"❌ خطأ في الاتصال: {e}")

# ==========================================

# ==========================================
print("⏳ جاري إنشاء الجداول...")

# --- Dimensions ---

# 1. Dim_Customer
client.execute('DROP TABLE IF EXISTS Dim_Customer')
client.execute('''
    CREATE TABLE Dim_Customer (
        customer_id Int32,
        age Int32,
        gender String,
        income Float64,
        offer_response_rate Float64
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

# 2. Dim_Device
client.execute('DROP TABLE IF EXISTS Dim_Device')
client.execute('''
    CREATE TABLE Dim_Device (
        customer_id Int32,
        device_type String,
        num_devices_registered Int32,
        device_age_months Int32,
        network_generation String,
        os_type String,
        uses_5g UInt8,
        device_security_patch String
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

# 3. Dim_Location
client.execute('DROP TABLE IF EXISTS Dim_Location')
client.execute('''
    CREATE TABLE Dim_Location (
        tower_id Int32,
        city String,
        state String,
        zip_code Int32,
        latitude Float64,
        longitude Float64,
        urbanization_level String
    ) ENGINE = MergeTree() ORDER BY tower_id
''')

client.execute('DROP TABLE IF EXISTS Dim_Time')
client.execute('''
    CREATE TABLE Dim_Time (
        date_id Date,
        year Int32,
        month Int32,
        day Int32,
        quarter Int32,
        day_name String,
        is_weekend UInt8
    ) ENGINE = MergeTree() ORDER BY date_id
''')

# --- Facts ---

# 5. Fact_CDR
client.execute('DROP TABLE IF EXISTS Fact_CDR')
client.execute('''
    CREATE TABLE Fact_CDR (
        customer_id Int32,
        monthly_call_count Int32,
        monthly_call_duration Int32,
        international_call_duration Int32
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

# 6. Fact_Payments
client.execute('DROP TABLE IF EXISTS Fact_Payments')
client.execute('''
    CREATE TABLE Fact_Payments (
        customer_id Int32,
        monthly_spending Int32,
        credit_score Int32,
        payment_method String
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

# 7. Fact_Complaints
client.execute('DROP TABLE IF EXISTS Fact_Complaints')
client.execute('''
    CREATE TABLE Fact_Complaints (
        customer_id Int32,
        customer_satisfaction_score Float64,
        customer_complaints Int32,
        technical_support_rating Int32,
        churn_risk_score Float64,
        has_contacted_support UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

# 8. Fact_Internet_Usage
client.execute('DROP TABLE IF EXISTS Fact_Internet_Usage')
client.execute('''
    CREATE TABLE Fact_Internet_Usage (
        customer_id Int32,
        internet_usage_gb Float64,
        internet_speed_avg_mbps Float64,
        latency_ms Float64,
        peak_usage_hour Int32,
        weekend_data_ratio Float64
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

# 9. Fact_Network_Metrics
client.execute('DROP TABLE IF EXISTS Fact_Network_Metrics')
client.execute('''
    CREATE TABLE Fact_Network_Metrics (
        tower_id Int32,
        signal_strength_avg Float64,
        network_downtime_minutes Int32,
        network_congestion_level String,
        avg_signal_strength Float64
    ) ENGINE = MergeTree() ORDER BY tower_id
''')

# 10. Fact_Security
client.execute('DROP TABLE IF EXISTS Fact_Security')
client.execute('''
    CREATE TABLE Fact_Security (
        customer_id Int32,
        num_failed_logins Int32,
        num_fraud_attempts Int32,
        encryption_type String,
        biometric_auth String,
        two_factor_auth UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id
''')

print("✅ تم إنشاء جميع الجداول (All Tables Created Successfully).")


# In[5]:


# ==========================================

# ==========================================
def insert_safe(spark_df, table_name, batch_size=50000):
    print(f"🚀 بدء نقل جدول: {table_name} ...")
    try:

        client.execute(f'TRUNCATE TABLE IF EXISTS {table_name}')

        batch = []
        total_inserted = 0

        for row in spark_df.toLocalIterator():
            batch.append(row)

            if len(batch) >= batch_size:
                client.execute(f'INSERT INTO {table_name} VALUES', batch)
                total_inserted += len(batch)
                print(f"   -> تم نقل {total_inserted} صف...")
                batch = [] 
        if batch:
            client.execute(f'INSERT INTO {table_name} VALUES', batch)
            total_inserted += len(batch)

        print(f"✅ تم الانتهاء من {table_name}: إجمالي {total_inserted} صف.\n")

    except Exception as e:
        print(f"❌ خطأ أثناء نقل {table_name}: {e}")



# In[3]:


import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from clickhouse_driver import Client

# ==========================================

# ==========================================
print("🔄 جاري تشغيل Spark بالإعدادات القصوى...")

spark = SparkSession.builder \
    .appName("Final Migration Fix") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .config("spark.driver.maxResultSize", "0") \
    .getOrCreate()

print("✅ Spark اشتغل والـ Buffer جاهز!")

# ==========================================

# ==========================================
CLICKHOUSE_IP = '172.23.0.3'
client = Client(host=CLICKHOUSE_IP, port=9000, user='admin', password='admin123', database='default')

# ==========================================

# ==========================================
def insert_safe(spark_df, table_name, batch_size=50000):
    print(f"🚀 بدء نقل جدول: {table_name} ...")
    try:

        client.execute(f'TRUNCATE TABLE IF EXISTS {table_name}')

        batch = []
        total_inserted = 0

        for row in spark_df.toLocalIterator():
            batch.append(row)
            if len(batch) >= batch_size:
                client.execute(f'INSERT INTO {table_name} VALUES', batch)
                total_inserted += len(batch)
                batch = []

        if batch:
            client.execute(f'INSERT INTO {table_name} VALUES', batch)
            total_inserted += len(batch)

        print(f"✅ تم الانتهاء من {table_name}: {total_inserted} صف.")

    except Exception as e:
        print(f"❌ خطأ في {table_name}: {e}")


# In[4]:


# --- A. Dimensions ---

insert_safe(final_dim_customer, 'Dim_Customer')
insert_safe(final_dim_device, 'Dim_Device')
insert_safe(final_dim_location, 'Dim_Location')
insert_safe(final_dim_time, 'Dim_Time')  

# --- B. Facts ---

insert_safe(fact_cdr, 'Fact_CDR')
insert_safe(fact_complaints, 'Fact_Complaints')
insert_safe(fact_payments, 'Fact_Payments')
insert_safe(fact_internet, 'Fact_Internet_Usage')
insert_safe(fact_network, 'Fact_Network_Metrics')
insert_safe(fact_security, 'Fact_Security')

print("\n🎉🎉 DONE!.")


# In[ ]:



