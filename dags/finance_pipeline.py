from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import clickhouse_connect
import os

DATA_PATH = "/opt/airflow/data_lake"

CH_HOST = "clickhouse-server"
CH_PORT = 8123
CH_USER = "admin"
CH_PASS = "rahasia"
CH_DB   = "dustinia"

default_args = {"owner": "finance_analyst", "retries": 1}

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )

# ─── TASK 1: Buat database & tabel di ClickHouse ───────────────────────────
def create_tables():
    client = get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.dim_customers (
            customer_id        String,
            customer_city      String,
            customer_state     String
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    """)

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.dim_payments (
            order_id              String,
            payment_type          String,
            payment_installments  Int32,
            payment_value         Float64
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.fact_orders (
            order_id         String,
            customer_id      String,
            order_status     String,
            order_total      Float64,
            order_date       Date
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.fact_high_value_customers (
            customer_id       String,
            total_spend       Float64,
            total_orders      Int32,
            customer_segment  String,
            customer_city     String,
            customer_state    String
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    """)
    print("✅ Semua tabel berhasil dibuat")

# ─── TASK 2: Load dim_customers ────────────────────────────────────────────
def load_customers():
    df = pd.read_csv(f"{DATA_PATH}/customers.csv")
    df = df[["customer_id", "customer_city", "customer_state"]].drop_duplicates()
    df = df.fillna("")

    client = get_client()
    client.command(f"TRUNCATE TABLE IF EXISTS {CH_DB}.dim_customers")
    client.insert_df(f"{CH_DB}.dim_customers", df)
    print(f"✅ dim_customers: {len(df)} rows loaded")

# ─── TASK 3: Load dim_payments ─────────────────────────────────────────────
def load_payments():
    df = pd.read_csv(f"{DATA_PATH}/order_payments.csv")
    df = df[["order_id", "payment_type", "payment_installments", "payment_value"]]
    df["payment_installments"] = df["payment_installments"].fillna(0).astype(int)
    df["payment_value"] = df["payment_value"].fillna(0.0)
    df = df.fillna("")

    client = get_client()
    client.command(f"TRUNCATE TABLE IF EXISTS {CH_DB}.dim_payments")
    client.insert_df(f"{CH_DB}.dim_payments", df)
    print(f"✅ dim_payments: {len(df)} rows loaded")

# ─── TASK 4: Load fact_orders ──────────────────────────────────────────────
def load_orders():
    orders = pd.read_csv(f"{DATA_PATH}/orders.csv")
    items  = pd.read_csv(f"{DATA_PATH}/order_items.csv")

    order_total = items.groupby("order_id")["price"].sum().reset_index()
    order_total.rename(columns={"price": "order_total"}, inplace=True)

    df = orders.merge(order_total, on="order_id", how="left")
    df = df[["order_id", "customer_id", "order_status",
             "order_total", "order_purchase_timestamp"]]
    df["order_total"] = df["order_total"].fillna(0.0)
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    )
    df["order_date"] = df["order_purchase_timestamp"].dt.date
    df = df.drop(columns=["order_purchase_timestamp"])
    df = df.dropna(subset=["order_date"])
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df.fillna("")

    client = get_client()
    client.command(f"TRUNCATE TABLE IF EXISTS {CH_DB}.fact_orders")
    client.insert_df(f"{CH_DB}.fact_orders", df)
    print(f"✅ fact_orders: {len(df)} rows loaded")

# ─── TASK 5: Load fact_high_value_customers ────────────────────────────────
def load_high_value():
    orders    = pd.read_csv(f"{DATA_PATH}/orders.csv")
    items     = pd.read_csv(f"{DATA_PATH}/order_items.csv")
    customers = pd.read_csv(f"{DATA_PATH}/customers.csv")

    order_total = items.groupby("order_id")["price"].sum().reset_index()
    order_total.rename(columns={"price": "order_total"}, inplace=True)

    df = orders.merge(order_total, on="order_id", how="left")
    df = df.merge(customers, on="customer_id", how="left")

    customer_value = df.groupby("customer_id").agg(
        total_spend=("order_total", "sum"),
        total_orders=("order_id", "count"),
        customer_city=("customer_city", "first"),
        customer_state=("customer_state", "first")
    ).reset_index()

    threshold = customer_value["total_spend"].quantile(0.90)
    customer_value["customer_segment"] = customer_value["total_spend"].apply(
        lambda x: "High Value" if x >= threshold else "Regular"
    )
    customer_value["total_spend"]  = customer_value["total_spend"].fillna(0.0)
    customer_value["total_orders"] = customer_value["total_orders"].fillna(0).astype(int)
    customer_value = customer_value.fillna("")

    client = get_client()
    client.command(f"TRUNCATE TABLE IF EXISTS {CH_DB}.fact_high_value_customers")
    client.insert_df(f"{CH_DB}.fact_high_value_customers", customer_value)
    print(f"✅ fact_high_value_customers: {len(customer_value)} rows loaded")

# ─── DAG Definition ────────────────────────────────────────────────────────
with DAG(
    dag_id="finance_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["finance", "dustinia"]
) as dag:

    t1 = PythonOperator(task_id="create_tables",            python_callable=create_tables)
    t2 = PythonOperator(task_id="load_customers",           python_callable=load_customers)
    t3 = PythonOperator(task_id="load_payments",            python_callable=load_payments)
    t4 = PythonOperator(task_id="load_orders",              python_callable=load_orders)
    t5 = PythonOperator(task_id="load_high_value_customers",python_callable=load_high_value)

    t1 >> [t2, t3, t4] >> t5