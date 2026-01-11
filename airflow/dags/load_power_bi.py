from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# === Cấu hình DAG ===
default_args = {
    'owner': 'odap_tambeo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='send_data_to_powerbi',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Mỗi 10 phút
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['powerbi', 'hdfs', 'odap']
)

# Lấy đường dẫn thư mục dự án
dag_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(dag_dir, '..', '..'))
path_compact_csv = os.path.join(project_dir, 'hadoop', 'compact_csv.py')
path_load_data = os.path.join(project_dir, 'powerbi', 'load_data.py')

# Task 1: Gộp các batch CSV thành 1 file compacted
compact_task = BashOperator(
    task_id='compact_csv_to_one_file',
    bash_command=f'spark-submit {path_compact_csv} && sleep 5',
    dag=dag
)

# Task 2: Load dữ liệu lên Power BI
load_data_task = BashOperator(
    task_id='load_data_power_bi',
    bash_command=f'python3 {path_load_data}',
    dag=dag
)

# Thiết lập thứ tự thực hiện
compact_task >> load_data_task


