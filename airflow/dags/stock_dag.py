from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_etl',
    default_args=default_args,
    description='Stock ETL pipeline',
    schedule_interval=None,
    start_date=datetime(2025, 9, 23),
    catchup=False,
) as dag:
    scrape_stock = BashOperator(
        task_id='scrape_stock',
        bash_command='python /mnt/d/projects/stock_project/scripts/scrape_stock.py',
    )
    run_mapreduce = BashOperator(
        task_id='run_mapreduce',
        bash_command='hadoop fs -rm -r /data/raw/stock_IBM_*.csv || true && hadoop fs -put /mnt/d/projects/stock_project/data/stock_IBM_*.csv /data/raw/ && hadoop fs -rm -r /data/cleaned_stock || true && hadoop jar /mnt/d/projects/stock_project/scripts/stock.jar StockClean /data/raw/stock_IBM_*.csv /data/cleaned_stock',
    )
    run_hive = BashOperator(
        task_id='run_hive',
        bash_command='hive -f /mnt/d/projects/stock_project/scripts/hive_query.sql',
    )
    run_pyspark = BashOperator(
        task_id='run_pyspark',
        bash_command='export HADOOP_CONF_DIR=/mnt/d/projects/stock_project/hadoop/etc/hadoop/ && spark-submit --master yarn --deploy-mode client --conf spark.eventLog.enabled=false /mnt/d/projects/stock_project/scripts/etl.py',
    )
    measure_before = PostgresOperator(
        task_id='measure_before',
        postgres_conn_id='postgres_default',
        sql="""
        EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';
        """,
        dag=dag
    )
    optimize_indexes = PostgresOperator(
        task_id='optimize_indexes',
        postgres_conn_id='postgres_default',
        sql="""
        -- Ensure tstamp is TIMESTAMP
        ALTER TABLE stock_data ALTER COLUMN tstamp TYPE TIMESTAMP USING tstamp::TIMESTAMP;
        -- Partition by tstamp (ngày)
        ALTER TABLE stock_data ADD COLUMN IF NOT EXISTS tstamp_date DATE GENERATED ALWAYS AS (DATE(tstamp)) STORED;
        CREATE INDEX IF NOT EXISTS idx_stock_data_tstamp_date ON stock_data (tstamp_date);
        CREATE INDEX IF NOT EXISTS idx_stock_data_tstamp ON stock_data (tstamp);
        CREATE INDEX IF NOT EXISTS idx_stock_data_volume ON stock_data (volume);
        """,
        dag=dag
    )
    vacuum_analyze = PostgresOperator(
        task_id='vacuum_analyze',
        postgres_conn_id='postgres_default',
        sql="""
        VACUUM ANALYZE stock_data;
        """,
        autocommit=True,
        dag=dag
    )

    measure_after = PostgresOperator(
        task_id='measure_after',
        postgres_conn_id='postgres_default',
        sql="""
        EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';
        """,
        dag=dag
    )

    scrape_stock >> run_mapreduce >> run_hive >> run_pyspark >> measure_before >> optimize_indexes >> vacuum_analyze >> measure_after