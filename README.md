# MedAirflow: Healthcare Data Pipeline

This project demonstrates an **automated data pipeline** using **Apache Airflow** that:

1. Generates synthetic healthcare appointment data using `Faker`.
2. Saves the data as `.csv` locally and uploads it to **Amazon S3**.
3. Loads data from S3 to **Snowflake** in a structured manner:
   - Raw Layer
   - Stage Layer (with transformations)
   - Main Layer (final table with clean data)

## 📌 Features

- 🔁 Daily DAG automation with Apache Airflow
- ☁️ Data storage on AWS S3
- ❄️ Snowflake integration (Raw → Stage → Main)
- 🧪 Dummy data generation for testing
- 🔄 Modular Python codebase

## 🛠️ Technologies Used

- Apache Airflow
- Python (Pandas, Faker, Boto3, PyODBC)
- Amazon S3
- Snowflake Cloud Data Warehouse

## 🚀 Setup Instructions

1. Clone this repo
2. Configure Airflow & AWS credentials
3. Set up Snowflake stage and integration
4. Trigger the DAG in Airflow

---

## 👤 Author

**Muhammad Zamin**  
Cloud Data Engineer | [LinkedIn](https://www.linkedin.com/in/mzamin-dataengnieer/)
