from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import re
import os

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
    
    # Measure before: Use BashOperator to capture output to file
    measure_before = BashOperator(
        task_id='measure_before',
        bash_command="""
        psql -U datauser -d airflow -h localhost -c "EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';" > /tmp/measure_before.log 2>&1
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

    # Measure after: Use BashOperator to capture output to file
    measure_after = BashOperator(
        task_id='measure_after',
        bash_command="""
        psql -U datauser -d airflow -h localhost -c "EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';" > /tmp/measure_after.log 2>&1
        """,
        dag=dag
    )

    # New task: Parse files and print BEFORE_TIME, AFTER_TIME, and % reduction
    def parse_and_print_times(**context):
        before_log = '/tmp/measure_before.log'
        after_log = '/tmp/measure_after.log'
        
        # Read before log
        if os.path.exists(before_log):
            with open(before_log, 'r') as f:
                before_content = f.read()
            before_match = re.search(r'Execution Time: ([\d.]+) ms', before_content)
            before_time = float(before_match.group(1)) if before_match else None
        else:
            before_time = None
        
        # Read after log
        if os.path.exists(after_log):
            with open(after_log, 'r') as f:
                after_content = f.read()
            after_match = re.search(r'Execution Time: ([\d.]+) ms', after_content)
            after_time = float(after_match.group(1)) if after_match else None
        else:
            after_time = None
        
        # Print times
        print(f"BEFORE_TIME: {before_time} ms" if before_time else "BEFORE_TIME: Not found")
        print(f"AFTER_TIME: {after_time} ms" if after_time else "AFTER_TIME: Not found")
        
        # Calculate % reduction if both available
        if before_time and after_time and before_time > 0:
            reduction = ((before_time - after_time) / before_time) * 100
            print(f"Phần trăm giảm: {reduction:.2f}%")
        else:
            print("Không thể tính % giảm (thiếu before/after time).")
        
        # Cleanup temp files
        if os.path.exists(before_log):
            os.remove(before_log)
        if os.path.exists(after_log):
            os.remove(after_log)

    print_times_task = PythonOperator(
        task_id='print_times',
        python_callable=parse_and_print_times,
        dag=dag
    )

    scrape_stock >> run_mapreduce >> run_hive >> run_pyspark >> measure_before >> optimize_indexes >> vacuum_analyze >> measure_after >> print_times_task