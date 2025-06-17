from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from faker import Faker
import os
from dotenv import load_dotenv

load_dotenv()

# ---- DAG Default Args ----
default_args = {
    'owner': 'zamin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='healthcare_daily_to_s3_only',
    default_args=default_args,
    description='Generate appointments and upload only to S3',
    start_date=datetime(2025, 5, 22),
    schedule_interval='@daily',
    catchup=False
)

# ---- Generate CSV ----
def generate_and_return_filename():
    fake = Faker()
    data = []
    for _ in range(50):
        data.append({
            "appointment_id": fake.uuid4(),
            "patient_id": fake.random_int(min=1000, max=9999),
            "doctor_id": fake.random_int(min=100, max=199),
            "department": fake.random_element(elements=["Cardiology", "ENT", "Neurology", "Orthopedics"]),
            "status": fake.random_element(elements=["Scheduled", "Cancelled", "No-show"]),
            "appointment_date": datetime.now().strftime("%Y-%m-%d"),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    df = pd.DataFrame(data)
    filename = f"/tmp/appointments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ File generated: {filename}")
    return filename

# ---- Upload to S3 Only ----
def upload_to_s3_only(**context):
    filename = context['ti'].xcom_pull(task_ids='generate_file')
    s3_path = f"{os.getenv('S3_FOLDER')}{os.path.basename(filename)}"

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        s3.upload_file(filename, os.getenv('S3_BUCKET_NAME'), s3_path)
        print(f"✅ Uploaded to S3: s3://{os.getenv('S3_BUCKET_NAME')}/{s3_path}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

    # Cleanup local file
    os.remove(filename)
    print("🧹 Deleted local file.")

# ---- Airflow Tasks ----
generate_task = PythonOperator(
    task_id='generate_file',
    python_callable=generate_and_return_filename,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3_only,
    provide_context=True,
    dag=dag
)

generate_task >> upload_task