##🌟 Building a Scalable Flights File Ingestion Pipeline ✈️📊

I’m excited to share details about my recent data engineering project: designing and implementing a Flights File Ingestion Model. This pipeline automates the ingestion and processing of flight data, ensuring a seamless flow from file upload to transformation and integration into our data warehouse.

##🚀 Overview of the Architecture
This architecture is built with scalability, reliability, and monitoring in mind. Here’s how the data flows:

![pipeline](https://github.com/dibnyk/project/blob/main/Nasa%20Mars%20Data/flow_diagram.png)

🔗 Key Workflow Steps

1️⃣ File Upload
Incoming flight data files are uploaded to an Amazon S3 bucket, serving as the raw data storage.


2️⃣ Event Monitoring
AWS CloudTrail monitors file activity in S3, triggering CloudWatch Events to detect uploads in real time.


3️⃣ Processing Orchestration
AWS Step Functions orchestrate the pipeline, invoking an AWS Lambda function to process files:
Data is extracted, validated, and transformed.
Processed data is stored in Snowflake tables for further analysis.


4️⃣ Data Storage
Snowflake is used to store and manage processed flight data:
Ingestion Table: Contains raw, processed data.
DIM Table: Stores cleaned and structured dimensional data for downstream analytics.


5️⃣ Monitoring & Alerts
Success Path: If the process completes successfully, the pipeline outputs results to the required Snowflake tables.
Failure Handling: Any errors trigger a failure workflow with notifications sent via SNS for quick resolution.


🌟 Key Highlights

Serverless Architecture: Utilized AWS services like Lambda and Step Functions to minimize infrastructure overhead and maximize scalability.

Real-Time Processing: The pipeline processes data as soon as files are uploaded, reducing latency.

Alerting Mechanism: Integrated Amazon SNS for proactive notifications on both success and failure events.

Data Warehouse Integration: Leveraged Snowflake’s power to enable rich analytics and reporting on flight data.


🛠️ Tech Stack
AWS: S3, CloudTrail, CloudWatch, Step Functions, Lambda, SNS
Snowflake: Data storage and querying


🎯 Impact
This ingestion model ensures:

Faster Data Availability: Enables real-time insights with reduced delays.
Improved Data Quality: Validations at each step ensure clean, reliable data for analytics.
Operational Efficiency: Automation and monitoring eliminate manual intervention and speed up processing.