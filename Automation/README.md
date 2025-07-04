# 🩺 AWS Synthea Healthcare Data Pipeline

This guide will walk you through setting up a complete ETL pipeline for **Synthea synthetic healthcare data** using:
- AWS Glue (ETL)
- AWS Lambda (trigger)
- Amazon S3 (storage)
- Amazon Athena (query)
- Power BI (visualization)

---

## 🧩 Task 1: Generate Synthea Data (CSV Format)

### 1️⃣ Download and Setup Synthea
- Download the Synthea project ZIP from GitHub:  
  👉 https://github.com/synthetichealth/synthea

- Extract the folder and navigate to:

```
synthea/src/main/resources/synthea.properties
```

- **Enable CSV Output**:  
  Open `synthea.properties` and set:
  ```properties
  exporter.csv.export = true
  ```

### 2️⃣ Run the Synthea Generator
Open terminal in the Synthea root directory and run:

```bash
./run_synthea -p <NUMBER_OF_PATIENTS>
```

📁 After completion, you'll see an `output/csv` folder with **18 CSV files**.

![CSV Output Example](images/synthea_output_folder.png)

---

## ⚙️ Task 2: AWS Glue + Lambda Automation

### 1️⃣ Create an S3 Bucket

- Go to **S3** and create a bucket (e.g., `synthea-data-pipeline`)
- Inside it, create these folders:
  ```
  /incoming/
  /processed/
  /errors/
  ```

> 📷 Refer to image below for example folder structure:

![S3 Folder Structure](images/s3_structure.png)

---

### 2️⃣ Create AWS Glue Jobs

- Go to **AWS Glue → Jobs** and create Glue ETL scripts
- Set:
  - **Number of Workers**: e.g., 10
  - **Worker Type**: G.1X or G.2X
  - **Concurrency**: Max 18 for Job 1
  - Add required **environment variables**

---

### 3️⃣ Add Lambda Trigger

- Create a **Lambda function** to trigger Glue Job
- Increase Lambda timeout (e.g., 5 mins or more)
- Grant it proper **IAM permissions** to start Glue jobs

> ✅ **Attach the following IAM Policies**:
- `AWSGlueServiceRole`
- `AmazonS3FullAccess`
- `AWSLambdaBasicExecutionRole`
- `CloudWatchLogsFullAccess`

---

### 4️⃣ Add Event Notification to S3

Go to:

**S3 → Properties → Event Notifications → Add Event**

- Name: `TriggerLambda`
- Event Type: `PUT`
- Prefix: `incoming/`
- Destination: Select your Lambda function

---

### 5️⃣ Create Glue Workflow

Go to **AWS Glue → Workflows**  
- Create a new workflow and attach your Glue jobs in sequence.

> 📷 Refer to the image below for an example Glue Workflow setup:

![Glue Workflow Setup](images/glue_workflow.png)

---

## 🧪 Task 3: Run the Pipeline

### ✅ Add Input Data

- Upload your **18 CSV files** into `incoming/` folder in your S3 bucket.

---

### 🛠 Monitor Execution

- **Lambda Logs**:  
  Go to **CloudWatch → Log Groups → /aws/lambda/<your_lambda>**

- **Glue Job Monitoring**:  
  Go to **AWS Glue → Jobs → Monitor Runs**

> 🔄 Refresh the page manually if Glue run status doesn’t auto-update.

---

### 🔍 Query with Amazon Athena

1. Go to **Athena → Settings** and set query result location (e.g., `s3://synthea-data-pipeline/athena-results/`)
2. Select the database created by Glue jobs
3. Write SQL queries to explore the data

---

## 📊 Visualize in Power BI

Now that your data is in Athena:
1. Install the [Athena ODBC Driver](https://docs.aws.amazon.com/athena/latest/ug/athena-odbc.html)
2. Create a DSN and connect it to Athena
3. Open **Power BI → Get Data → ODBC → Athena DSN**
4. Load your datasets and create reports!

---

## 🧾 Notes

- Always test Lambda with smaller datasets first
- Ensure S3, Glue, and Lambda are in the **same region**
- Update concurrency and memory limits based on job performance
- Monitor failed job runs in **CloudWatch Logs**

---

## 📁 Folder Reference

```
synthea/
├── output/
│   └── csv/
│       ├── patients.csv
│       ├── conditions.csv
│       └── ...
```

```
S3 Bucket: synthea-data-pipeline/
├── incoming/
├── processed/
├── errors/
└── athena-results/
```