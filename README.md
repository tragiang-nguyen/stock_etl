# ETL Stock Pipeline

[![CI/CD Status](https://github.com/tragiang-nguyen/stock_etl/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/tragiang-nguyen/stock_etl/actions/workflows/ci-cd.yml)

## Tổng Quan
Dự án này là một pipeline ETL (Extract, Transform, Load) end-to-end đầy đủ cho dữ liệu cổ phiếu IBM, sử dụng Apache Airflow để điều phối, Hadoop HDFS/MapReduce/Hive cho xử lý batch, PySpark để load dữ liệu, và PostgreSQL làm kho dữ liệu (DW). Pipeline scrape dữ liệu cổ phiếu, làm sạch, tổng hợp (trung bình close theo timestamp), load vào DW, và tối ưu hóa truy vấn lên **53.55%** bằng index và partition (before: 0.282 ms, after: 0.131 ms).

Các tính năng chính:
- Xử lý batch có khả năng mở rộng với hệ sinh thái Hadoop.
- Điều phối tự động với Airflow (retries=5, parallelism=32).
- Tối ưu hóa truy vấn: Giảm thời gian execution từ 0.282 ms xuống 0.131 ms (cải thiện 53.55%).
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
   - Sửa `./hadoop/etc/hadoop/core-site.xml` (dựa trên config thực tế của bạn):
     ```
     <?xml version="1.0" encoding="UTF-8"?>
     <configuration>
         <property>
             <name>fs.defaultFS</name>
             <value>hdfs://localhost:9000</value>
         </property>
         <property>
             <name>hadoop.tmp.dir</name>
             <value>/home/user/hadoop-tmp</value>
         </property>
     </configuration>
     ```
   - Sửa `./hadoop/etc/hadoop/hdfs-site.xml` (dựa trên config thực tế):
     ```
     <?xml version="1.0" encoding="UTF-8"?>
     <configuration>
         <property>
             <name>dfs.replication</name>
             <value>1</value>
         </property>
         <property>
             <name>dfs.namenode.name.dir</name>
             <value>file:${hadoop.tmp.dir}/dfs/name</value>
         </property>
         <property>
             <name>dfs.datanode.data.dir</name>
             <value>file:${hadoop.tmp.dir}/dfs/data</value>
         </property>
     </configuration>
     ```
   - Format HDFS và start (dựa trên thao tác thực tế):
     ```
     $HADOOP_HOME/bin/hdfs namenode -format
     $HADOOP_HOME/sbin/start-dfs.sh
     $HADOOP_HOME/sbin/start-yarn.sh
     ```
     - Kiểm tra: `jps` (NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager).

4. **Cài Hive**:
   - Download binary Hive 2.3.9 từ [hive.apache.org](https://hive.apache.org/downloads.html), giải nén vào `./apache-hive-2.3.9-bin`.
   - Sửa `./apache-hive-2.3.9-bin/conf/hive-site.xml` (dựa trên config thực tế của bạn):
     ```
     <?xml version="1.0"?>
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
         <property>
             <name>hive.metastore.schema.verification</name>
             <value>false</value>
         </property>
         <property>
           <name>hive.metastore.uris</name>
           <value>thrift://localhost:9083</value>
         </property>
     </configuration>
     ```
   - Init schema (lần đầu):
     ```
     $HIVE_HOME/bin/schematool -dbType postgres -initSchema
     ```
   - Start Metastore (dựa trên thao tác thực tế):
     ```
     nohup $HIVE_HOME/bin/hive --service metastore > hive-metastore.log 2>&1 &
     ```
     - Kiểm tra: `ps aux | grep metastore` (RunJar process), `tail -f hive-metastore.log` (Starting Hive Metastore Server).

5. **Cài PostgreSQL**:
   - Cài: `sudo apt install postgresql postgresql-contrib`
   - Restart (dựa trên thao tác thực tế):
     ```
     sudo service postgresql restart
     sudo service postgresql status
     ```
   - Tạo DB/user:
     ```
     sudo -u postgres psql
     CREATE USER datauser WITH PASSWORD 'password';
     CREATE DATABASE airflow OWNER datauser;
     \q
     ```

6. **Cài Airflow**:
   - Tạo venv: `python3 -m venv /home/user/airflow_env && source /home/user/airflow_env/bin/airflow activate`
   - Cài: `pip install apache-airflow[postgres]`
   - Config `airflow.cfg`: executor = SequentialExecutor, sql_alchemy_conn = postgresql://datauser:password@localhost/airflow
   - Init DB: `airflow db init`
   - Start (dựa trên thao tác thực tế):
     ```
     sudo -E /home/user/airflow_env/bin/airflow webserver -p 8080 > airflow-webserver.log 2>&1 &
     nohup /home/user/airflow_env/bin/airflow scheduler > airflow-scheduler.log 2>&1 &
     ```
     - Kiểm tra: `ps aux | grep airflow` (gunicorn workers, scheduler), `tail -f airflow-scheduler.log` (Starting the scheduler).

## Chạy Dự Án
1. **Start Services** (Hadoop, Hive, PostgreSQL, Airflow – xem Cài Đặt).
   - Hadoop: `$HADOOP_HOME/sbin/start-dfs.sh && $HADOOP_HOME/sbin/start-yarn.sh`
   - Hive: `nohup $HIVE_HOME/bin/hive --service metastore > hive-metastore.log 2>&1 &`
   - PostgreSQL: `sudo service postgresql restart`
   - Airflow: `sudo -E /home/user/airflow_env/bin/airflow webserver -p 8080 > airflow-webserver.log 2>&1 &` và `nohup /home/user/airflow_env/bin/airflow scheduler > airflow-scheduler.log 2>&1 &`
2. **Trigger DAG**:
   ```
   /home/user/airflow_env/bin/airflow dags trigger stock_etl
   ```
   - Theo dõi: http://localhost:8080 (login admin/admin), DAGs → stock_etl → Graph View.
   - Thời gian chạy: ~5-10 phút (scrape → load → optimize).

3. **Kiểm Tra Kết Quả**:
   - Dữ liệu loaded:
     ```
     psql -U datauser -d airflow -h localhost -c "SELECT COUNT(*) FROM stock_data;"
     ```
     - Kết quả mong đợi: ~4380 rows.
   - Hiệu suất truy vấn:
     ```
     psql -U datauser -d airflow -h localhost -c "EXPLAIN ANALYZE SELECT * FROM stock_data WHERE tstamp > '2025-10-01';"
     ```
     - Kết quả mong đợi: Execution Time ~0.131 ms (Index Scan).

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
   - Theo dõi log scheduler sau trigger:
     ```
     tail -f airflow/logs/scheduler.log | grep -i "BEFORE_TIME\|AFTER_TIME\|Phần trăm giảm"
     ```
     - Kết quả mong đợi: "BEFORE_TIME: 0.282 ms", "AFTER_TIME: 0.131 ms", "Phần trăm giảm: 53.55%".

3. **Test CI/CD** (GitHub Actions):
   - Push code → Tab Actions → "CI/CD ETL Stock Pipeline" tự chạy (test + lint pass, checkmark xanh).

## Xử Lý Lỗi
- **Airflow không start**: Kiểm tra log (`tail -f airflow/logs/scheduler.log`), chown airflow dir (`sudo chown -R user:user airflow`).
- **HDFS/Hive lỗi**: Start services (`start-dfs.sh`, hive --service metastore`), kiểm tra jps (`jps | grep NameNode`).
- **PostgreSQL conn fail**: Restart (`sudo service postgresql restart`), kiểm tra user/DB (`psql -U datauser -d airflow -h localhost`).
- **PySpark test fail**: Reinstall venv (`pip install pyspark==3.5.1 pytest`).
- **% Giảm không 53%**: Chạy sau DAG success (data/index update), dao động tùy load.

## Kết Quả
- **Dữ Liệu**: ~4380 rows loaded vào PostgreSQL DW.
- **Tối Ưu Hóa**: Thời gian truy vấn giảm 53.55% (before 0.282 ms, after 0.131 ms qua index/partition).
- **CI/CD**: GitHub Actions xanh (4 test pass, lint SQL success).

---
