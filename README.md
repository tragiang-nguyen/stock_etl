# Dự Án ETL Stock Pipeline

[![CI/CD Status](https://github.com/tragiang-nguyen/stock_etl/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/tragiang-nguyen/stock_etl/actions/workflows/ci-cd.yml)  
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Tổng Quan
Dự án này là một pipeline ETL (Extract, Transform, Load) end-to-end đầy đủ cho dữ liệu cổ phiếu IBM, sử dụng Apache Airflow để điều phối, Hadoop HDFS/MapReduce/Hive cho xử lý batch, PySpark để load dữ liệu, và PostgreSQL làm kho dữ liệu (DW). Pipeline scrape dữ liệu cổ phiếu, làm sạch, tổng hợp (trung bình close theo timestamp), load vào DW, và tối ưu hóa truy vấn lên **65.77%** bằng index và partition.

Các tính năng chính:
- Xử lý batch có khả năng mở rộng với hệ sinh thái Hadoop.
- Điều phối tự động với Airflow (retries=5, parallelism=32).
- Tối ưu hóa truy vấn: Giảm thời gian execution từ 0.26 ms xuống 0.089 ms (cải thiện 65.77%).
- CI/CD với GitHub Actions (unit test + lint SQL).
- Unit test cho PySpark ETL (4/4 pass).

Dự án mô phỏng hệ thống ETL sản xuất (e.g., GCP BigQuery ETL với Cloud Composer), phù hợp cho vị trí Junior Data Engineer.

## Công Nghệ Sử Dụng
- **Điều Phối**: Apache Airflow 2.x (DAGs, BashOperator, PostgresOperator, PythonOperator).
- **Xử Lý Batch**: Hadoop HDFS, MapReduce (Java), Apache Hive (tổng hợp SQL).
- **Xử Lý Dữ Liệu**: PySpark 3.5.1 (load JDBC vào PostgreSQL).
- **Kho Dữ Liệu**: PostgreSQL 16 (DW với index, cột generated cho partition).
- **Test**: pytest (unit test PySpark).
- **CI/CD**: GitHub Actions (test + lint trên push/PR).
- **Khác**: Python 3.12, Java 11 (cho MapReduce), Bash scripts.

## Cấu Trúc Dự Án
```
stock_project/
├── .github/workflows/ci-cd.yml          # Pipeline CI/CD GitHub Actions
├── .gitignore                           # Loại trừ binaries/logs
├── airflow/
│   ├── dags/
│   │   └── stock_dag.py                 # DAG Airflow (pipeline ETL)
│   ├── airflow.cfg                      # Config Airflow
│   └── logs/                            # Logs Airflow (loại trừ Git)
├── scripts/
│   ├── scrape_stock.py                  # Scrape dữ liệu cổ phiếu (Python)
│   ├── etl.py                           # PySpark ETL load vào PostgreSQL
│   ├── hive_query.sql                   # SQL Hive (tạo bảng, tổng hợp)
│   └── StockClean.java                  # Code MapReduce Java (làm sạch dữ liệu)
├── tests/
│   └── test_etl.py                      # Unit test PySpark (4/4 pass)
├── data/                                # Dữ liệu mẫu cổ phiếu (loại trừ Git)
├── apache-hive-2.3.9-bin/               # Binary Hive (loại trừ)
├── hadoop/                              # Binary Hadoop (loại trừ)
├── spark/                               # Binary Spark (loại trừ)
└── README.md                            # File này
```

## Yêu Cầu
- **OS**: Ubuntu 24.04 (WSL2 trên Windows khuyến nghị).
- **Python**: 3.12 (venv cho Airflow).
- **Java**: OpenJDK 11 (cho Hadoop/Hive/MapReduce).
- **PostgreSQL**: 16 (user `datauser` / password `password`, DB `airflow`).
- **Hadoop**: 3.x (HDFS, YARN, single-node).
- **Hive**: 2.3.9 (Metastore service).
- **Airflow**: 2.x (với provider postgres).
- **Git**: Đã cài (clone repo).

RAM: Ít nhất 8GB (cho Spark/Hadoop).

## Cài Đặt
1. **Clone Repo**:
   ```
   git clone https://github.com/tragiang-nguyen/stock_etl.git
   cd stock_etl
   ```

2. **Cài Java**:
   ```
   sudo apt update && sudo apt install openjdk-11-jdk
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
   source ~/.bashrc
   ```

3. **Cài Hadoop (Single-Node)**:
   - Download binary Hadoop 3.x từ [hadoop.apache.org](https://hadoop.apache.org/releases.html), giải nén vào `./hadoop`.
   - Sửa `./hadoop/etc/hadoop/core-site.xml`:
     ```
     <configuration>
       <property>
         <name>fs.defaultFS</name>
         <value>hdfs://localhost:9000</value>
       </property>
     </configuration>
     ```
   - Sửa `./hadoop/etc/hadoop/hdfs-site.xml`:
     ```
     <configuration>
       <property>
         <name>dfs.replication</name>
         <value>1</value>
       </property>
     </configuration>
     ```
   - Format HDFS và start:
     ```
     ./hadoop/bin/hdfs namenode -format
     ./hadoop/sbin/start-dfs.sh
     ./hadoop/sbin/start-yarn.sh
     ```

4. **Cài Hive**:
   - Download binary Hive 2.3.9 từ [hive.apache.org](https://hive.apache.org/downloads.html), giải nén vào `./apache-hive-2.3.9-bin`.
   - Sửa `./apache-hive-2.3.9-bin/conf/hive-site.xml`:
     ```
     <configuration>
       <property>
         <name>javax.jdo.option.ConnectionURL</name>
         <value>jdbc:postgresql://localhost:5432/airflow</value>
       </property>
       <property>
         <name>javax.jdo.option.ConnectionDriverName</name>
         <value>org.postgresql.Driver</value>
       </property>
       <property>
         <name>javax.jdo.option.ConnectionUserName</name>
         <value>datauser</value>
       </property>
       <property>
         <name>javax.jdo.option.ConnectionPassword</name>
         <value>password</value>
       </property>
     </configuration>
     ```
   - Start Metastore:
     ```
     nohup ./apache-hive-2.3.9-bin/bin/hive --service metastore > hive-metastore.log 2>&1 &
     ```

5. **Cài PostgreSQL**:
   - Cài: `sudo apt install postgresql postgresql-contrib`
   - Tạo DB/user:
     ```
     sudo -u postgres psql
     CREATE USER datauser WITH PASSWORD 'password';
     CREATE DATABASE airflow OWNER datauser;
     \q
     ```

6. **Cài Airflow**:
   - Tạo venv: `python3 -m venv /home/user/airflow_env && source /home/user/airflow_env/bin/activate`
   - Cài: `pip install apache-airflow[postgres]`
   - Config `airflow.cfg`: executor = SequentialExecutor, sql_alchemy_conn = postgresql://datauser:password@localhost/airflow
   - Init DB: `airflow db init`
   - Start:
     ```
     nohup airflow webserver -p 8080 > airflow-webserver.log 2>&1 &
     nohup airflow scheduler > airflow-scheduler.log 2>&1 &
     ```

## Chạy Dự Án
1. **Start Services** (Hadoop, Hive, PostgreSQL, Airflow – xem Cài Đặt).
2. **Trigger DAG**:
   ```
   airflow dags trigger stock_etl
   ```
   - Theo dõi: http://localhost:8080 (login admin/admin), DAGs → stock_etl → Graph View.
   - Thời gian chạy: ~5-10 phút (scrape → load → optimize).

3. **Kiểm Tra Kết Quả**:
   - Dữ liệu loaded:
     ```
     psql -U datauser -d airflow -h localhost -c "SELECT COUNT(*) FROM stock_data;"
     ```
     - Kết quả mong đợi: 4382 rows.
   - Hiệu suất truy vấn:
     ```
     psql -U datauser -d airflow -h localhost -c "EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';"
     ```
     - Kết quả mong đợi: Execution Time ~0.089 ms (Index Scan).

## Test Dự Án
1. **Unit Test (PySpark ETL)**:
   ```
   source /home/user/stock_test_env/bin/activate  # Venv với pyspark 3.5.1
   cd tests
   python -m pytest test_etl.py -v
   deactivate
   ```
   - Kết quả mong đợi: 4/4 PASSED (schema, cast, filter/order, mock JDBC).

2. **Kiểm Tra % Giảm Hiệu Suất** (Tối Ưu Hóa Truy Vấn):
   - Chạy query thủ công trước/sau optimize (thay thời gian nếu cần):
     ```
     # Before (tạm drop index)
     psql -U datauser -d airflow -h localhost -c "DROP INDEX IF EXISTS idx_stock_data_tstamp; EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01'; CREATE INDEX idx_stock_data_tstamp ON stock_data (tstamp);"
     # Copy "Execution Time: X.XX ms" làm before (e.g., 0.26 ms)

     # After (index có sẵn)
     psql -U datauser -d airflow -h localhost -c "EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';"
     # Copy "Execution Time: Y.YY ms" làm after (e.g., 0.089 ms)

     # Tính % giảm (thay X.XX, Y.YY)
     before=0.26; after=0.089; echo "scale=2; (($before - $after) / $before * 100)/1" | bc
     # Kết quả: 65.77
     ```

3. **Test CI/CD** (GitHub Actions):
   - Push code → Tab Actions → "CI/CD ETL Stock Pipeline" tự chạy (test + lint pass, checkmark xanh).

## Xử Lý Lỗi
- **Airflow không start**: Kiểm tra log (`tail -f airflow/logs/scheduler.log`).
- **HDFS/Hive lỗi**: Start services (`start-dfs.sh`, hive --service metastore`).
- **PostgreSQL conn fail**: Kiểm tra user/DB (`psql -U datauser -d airflow -h localhost`).
- **PySpark test fail**: Reinstall venv (`pip install pyspark==3.5.1 pytest`).
- **% Giảm không 65%**: Chạy sau DAG success (data/index update).

## Kết Quả
- **Dữ Liệu**: 4382 rows loaded vào PostgreSQL DW.
- **Tối Ưu Hóa**: Thời gian truy vấn giảm 65.77% (before 0.26 ms, after 0.089 ms qua index/partition).
- **CI/CD**: GitHub Actions xanh (4 test pass, lint SQL success).

---
