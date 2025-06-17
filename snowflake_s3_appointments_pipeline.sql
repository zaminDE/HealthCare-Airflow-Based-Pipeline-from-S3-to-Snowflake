-- STEP 1: Create FILE FORMAT for CSV
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

-- STEP 2: Create STORAGE INTEGRATION for S3
CREATE OR REPLACE STORAGE INTEGRATION S3_INT_HEALTHCARE
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::975268426540:role/s3-access-rolesnowflake'
  STORAGE_ALLOWED_LOCATIONS = ('s3://s3-healthcare-mz/appointments/');

-- STEP 3: Create STAGE to access files from S3
CREATE OR REPLACE STAGE s3_stage
  URL = 's3://s3-healthcare-mz/appointments/'
  STORAGE_INTEGRATION = S3_INT_HEALTHCARE
  FILE_FORMAT = csv_format;

-- STEP 4: Create RAW Table (raw dump of file)
CREATE OR REPLACE TABLE appointments_raw (
    appointment_id STRING,
    patient_id STRING,
    doctor_id STRING,
    department STRING,
    status STRING,
    appointment_date STRING,
    created_at STRING
);

-- STEP 5: Load data from S3 to RAW table
COPY INTO appointments_raw
FROM @s3_stage
PATTERN = '.*appointments_.*\.csv'
ON_ERROR = 'CONTINUE';

-- STEP 6: Create STAGE Table with type conversion + cleaning
CREATE OR REPLACE TABLE appointments_stage AS
SELECT
    appointment_id,
    TRY_CAST(patient_id AS INT) AS patient_id,
    TRY_CAST(doctor_id AS INT) AS doctor_id,
    department,
    status,
    TRY_CAST(appointment_date AS DATE) AS appointment_date,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at
FROM appointments_raw
WHERE TRY_CAST(patient_id AS INT) IS NOT NULL
  AND TRY_CAST(doctor_id AS INT) IS NOT NULL;

-- STEP 7: Create FINAL/MODEL Table (only once)
CREATE OR REPLACE TABLE appointments_main (
    appointment_id STRING,
    patient_id INT,
    doctor_id INT,
    department STRING,
    status STRING,
    appointment_date DATE,
    created_at TIMESTAMP
);

-- STEP 8: Insert clean data to MAIN Table
INSERT INTO appointments_main
SELECT * FROM appointments_stage;

-- (Optional) Preview final table
SELECT * FROM appointments_main LIMIT 10;
