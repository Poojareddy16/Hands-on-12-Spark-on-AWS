
# Serverless Spark ETL Pipeline on AWS
## 📌 Project Overview

This project implements a **fully automated, serverless ETL data pipeline** on AWS.  
When a **raw product reviews CSV** is uploaded to an S3 bucket, a series of automated steps are triggered:

1. **AWS Lambda** detects the file upload event.  
2. Lambda automatically **starts the AWS Glue ETL Job**.  
3. **AWS Glue (PySpark)** performs:
   - Data cleaning  
   - Transformations  
   - FOUR Spark SQL analytics queries  
   - Writes processed data & analytics results to S3  

---

## 📁 Architecture Diagram (High-Level)

**S3 (raw) → Lambda Trigger → Glue ETL (Spark) → S3 (processed + analytics)**

```bash
     +----------------------+
     |   S3 Raw Bucket      |
     |   handsonl13/raw     |
     +----------+-----------+
                |
                | S3:ObjectCreated event
                v
      +---------------------+
      |    AWS Lambda       |
      | start_glue_job()    |
      +----------+----------+
                 |
                 | glue:StartJobRun
                 v
        +---------------------+
        |     AWS Glue        |
        |   PySpark Script    |
        +----------+----------+
                 |
                 | processed data + SQL outputs
                 v
      +-------------------------------+
      |   S3 Processed + Analytics    |
      |  handsonl13/processed/        |
      |  handsonl13/athena-results/   |
      +-------------------------------+
```
---
## 🗂️ AWS S3 Bucket Structure (Final)

```bash

handsonl13/
│
├── raw/
│ └── reviews.csv
│
├── processed/
│ └── part-00000.csv
│
└── athena-results/
├── main-product-analytics/
│ └── part-00000.csv
├── datewise-review-count/
│ └── part-00000.csv
├── top5-customers/
│ └── part-00000.csv
└── rating-distribution/
└── part-00000.csv

```


---

## ⚙️ Services Used
- **AWS S3** – Storage for raw, processed, analytics layers  
- **AWS Lambda** – Automatically triggers Glue job  
- **AWS Glue (Spark)** – ETL + data analytics  
- **Spark SQL** – Aggregation queries  
- **CloudWatch** – Job monitoring  
- **IAM** – Roles & permissions  

---

## 🚀 Step-by-Step Implementation

### ✔ 1. Create S3 Buckets
```bash
handsonl13/raw
handsonl13/processed
handsonl13/athena-results
```

Upload **reviews.csv** into the **raw/** directory.

<img width="1913" height="1002" alt="image" src="https://github.com/user-attachments/assets/1de3cc22-9c67-4b18-be31-c4481571b758" />

---

### ✔ 2. Create IAM Role for Glue

A dedicated IAM role is required for AWS Glue so the ETL job can access S3 and run successfully.

#### 🟦 Role Name: AWSGlueServiceRole-Reviews

#### 🟩 Steps to Create the Role
1. Go to **IAM → Roles → Create Role**
2. Choose **AWS Service**
3. Select **Glue**
4. Click **Next**

#### 🟨 Attach the Required Policies
Attach the following AWS-managed policies:

```bash
AWSGlueServiceRole
AmazonS3FullAccess
```

These allow Glue to run jobs and access S3 buckets.

<img width="1919" height="1000" alt="image" src="https://github.com/user-attachments/assets/ebdeca5f-ee17-4377-9749-ea8ca5422fa3" />


---

### ✔ 3. Create the AWS Glue ETL Job

1. Open **AWS Glue → ETL Jobs**
2. Click **Create Job**
3. Choose **Spark Script Editor**
4. Paste the contents of your ETL script (`glue_job_script.py`)
5. Go to **Job Details**
6. Set: **Job Name →** process_reviews_job
7. **IAM Role →** AWSGlueServiceRole-Reviews
8. Click **Save**

The script is already configured to use your S3 paths:
```bash
s3://handsonl13/raw/
s3://handsonl13/processed/
s3://handsonl13/athena-results/
```
<img width="947" height="485" alt="glueJob" src="https://github.com/user-attachments/assets/58d08550-13b2-473a-8184-f0dfcc01558a" />


### ✔ 4. Create Lambda Trigger Function

This Lambda function automatically starts the AWS Glue ETL job whenever a new CSV file is uploaded into the S3 raw folder.

### 🟦 Lambda Function Name : start_glue_job_trigger

### 🟩 Steps to Create Lambda Function

1. Go to **AWS Lambda → Create Function**
2. Choose:
   - **Author from scratch**
   - Runtime: **Python 3.10 or newer**
3. Under permissions:
   - Select **Create a new role with basic Lambda permissions**
4. Click **Create Function**

<img width="956" height="446" alt="lambda-trigger" src="https://github.com/user-attachments/assets/91df83da-729b-46d4-ad8d-a2b6465077f4" />


---

### 5. a 🟦 Lambda Function Code (Paste in the Code Editor)

```python
import boto3

GLUE_JOB_NAME = "process_reviews_job"

def lambda_handler(event, context):
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)
    print("Glue job started:", response["JobRunId"])
    return {"status": "OK"}
```
This will:

- Receive S3 upload event
- Start the Glue job
- Print the Glue JobRunId in CloudWatch logs

### 5.b 🟧 Add Permissions to Allow Lambda to Start Glue Jobs

Lambda needs special permission to call Glue.

1. Go to:
     IAM → Roles → (your Lambda execution role)
2. Click Add permissions → Create inline policy
3. Choose the JSON tab
4. Paste the following:

```bash
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "glue:StartJobRun",
            "Resource": "*"
        }
    ]
}

```
5. Name the policy: Allow-Glue-StartJobRun
6. Save
Now Lambda can successfully start your Glue ETL job.

<img width="1914" height="994" alt="image" src="https://github.com/user-attachments/assets/4b5ced18-150a-4678-b9b2-dc142454477e" />

### 5.c 🟦 Add S3 Trigger to Lambda

To make Lambda run whenever a new file is uploaded:
1. Go to your Lambda function page
2. Scroll to Function overview
3. Click Add Trigger
4. Select:
  -  S3
  - Bucket: handsonl13
  - Event Type: All object create events
  - Prefix: raw/

5. Acknowledge warning → Click Add

This ensures Lambda triggers ONLY when a file lands in the correct folder:
```bash
handsonl13/raw/
```

--- 

### 🟩 Expected CloudWatch Log Output

Once the S3 → Lambda trigger activates, you can verify the execution by checking:

```bash
CloudWatch → Logs → /aws/lambda/start_glue_job_trigger
```
You will see an entry similar to:

- START RequestId: 7c1d5bdf-0d2a-4a3c-9af7-230e8932b26a Version: $LATEST
- Glue job started: jr_123456789abcdef
- END RequestId: 7c1d5bdf-0d2a-4a3c-9af7-230e8932b26a
- REPORT RequestId: 7c1d5bdf-0d2a-4a3c-9af7-230e8932b26a Duration: 125.34 ms Billed Duration: 126 ms

<img width="959" height="471" alt="cloudwatch" src="https://github.com/user-attachments/assets/c4213f48-a335-4f9c-954b-3614eab94b6b" />

---

### ✔ What This Confirms

- **S3 → Lambda Trigger is working**  
  Lambda ran automatically after the file upload.

- **Lambda → Glue integration is successful**  
  Lambda was able to start the Glue job.

- **Glue job was launched correctly**  
  The JobRunId (`jr_123456789abcdef`) confirms the ETL job started.

---


### 🚀 How to Run the Pipeline 

1️⃣ Upload your raw file to S3:
s3://handsonl13/raw/reviews.csv

2️⃣ This automatically triggers:
- S3 → Lambda  
- Lambda → Glue  
- Glue runs ETL + Spark SQL queries  

3️⃣ Monitor the Glue job:
AWS Glue → Jobs → process_reviews_job → Runs

Status should be **Succeeded**.

4️⃣ Check output in S3:
- s3://handsonl13/processed/
- s3://handsonl13/athena-results/
--- 

### 📈 Athena Results (Short Summary)

After the Glue ETL job finishes, all analytics outputs are written to:
```bash
s3://handsonl13/athena-results/
```

Glue automatically creates four folders—one for each Spark SQL query:
```bash
athena-results/
├── main-product-analytics/
├── datewise-review-count/
├── top5-customers/
└── rating-distribution/
```

- Each folder contains: part-00000.csv
<img width="946" height="496" alt="athena-results" src="https://github.com/user-attachments/assets/c324e995-59bb-49af-affd-e00e27849642" />
<img width="959" height="464" alt="s3-athena-results" src="https://github.com/user-attachments/assets/2d3387fc-5769-4c80-924c-672411b52fa2" />

---

### 📌 Tips for Verification
- If you do **not** see this log → Lambda did not trigger.  
- If the log shows an **AccessDenied** → Lambda role is missing permissions.  
- If Glue job does not appear in “Runs” → Check Glue job name in Lambda code.  



