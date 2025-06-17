from dotenv import load_dotenv
import os
import pandas as pd
import boto3
from faker import Faker
from datetime import datetime

# Load environment variables
load_dotenv()

def generate_and_upload_to_s3():
    print("📦 Generating fake data...")
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
    filename = f"appointments_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(filename, index=False)
    print(f"📁 CSV file created: {filename}")

    # Get environment vars
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    s3_path = f"appointments/{filename}"

    print("🔑 AWS Credentials loaded:")
    print(f"Key: {aws_access_key_id[:4]}****")  # just partially show for safety
    print(f"Bucket: {bucket_name}")
    print(f"Region: {region}")
    print(f"S3 Path: {s3_path}")

    # Upload to S3
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        print("🚀 Uploading to S3...")
        s3.upload_file(filename, bucket_name, s3_path)
        print(f"✅ Uploaded {filename} to S3 bucket {bucket_name}/{s3_path}")
    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")

    # Clean up
    os.remove(filename)
    print("🧹 Local file deleted.")

# Run function
generate_and_upload_to_s3()
