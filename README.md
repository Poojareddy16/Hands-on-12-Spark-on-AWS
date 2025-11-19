
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

---

### ✔ 3. Create Lambda Trigger Function

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

---

## 🟦 Lambda Function Code (Paste in the Code Editor)

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

## 🟧 Add Permissions to Allow Lambda to Start Glue Jobs

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
## 🟦 Add S3 Trigger to Lambda

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

---

### ✔ What This Confirms

- **S3 → Lambda Trigger is working**  
  Lambda ran automatically after the file upload.

- **Lambda → Glue integration is successful**  
  Lambda was able to start the Glue job.

- **Glue job was launched correctly**  
  The JobRunId (`jr_123456789abcdef`) confirms the ETL job started.

---

### 📌 Tips for Verification
- If you do **not** see this log → Lambda did not trigger.  
- If the log shows an **AccessDenied** → Lambda role is missing permissions.  
- If Glue job does not appear in “Runs” → Check Glue job name in Lambda code.  



