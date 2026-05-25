## STEDI Human Balance Analytics Data Lakehouse Project
Project Overview
This project implements a cloud-based data lakehouse solution for STEDI Human Balance Analytics using AWS services.
STEDI has developed a Step Trainer device equipped with motion sensors that collect user movement data. The company also provides a mobile application that captures accelerometer readings from users' mobile devices.
The objective of this project is to ingest, process, sanitize, and curate customer, accelerometer, and IoT step trainer data into machine-learning-ready datasets while ensuring customer privacy compliance.
The complete ETL pipeline was implemented using AWS Glue, Amazon S3, AWS Athena, and PySpark.
________________________________________
# Project Architecture
The project follows a multi-layer lakehouse architecture:

Landing Zone
-------------
customer_landing
accelerometer_landing
step_trainer_landing

        ↓

Trusted Zone
-------------
customer_trusted
accelerometer_trusted
step_trainer_trusted

        ↓

Curated Zone
-------------
customers_curated
machine_learning_curated
________________________________________
# Architecture Diagram
 <img width="900" height="1350" alt="image" src="https://github.com/user-attachments/assets/56dcd6ae-e79a-43f4-8f8f-548f345984a3" />

________________________________________
# AWS Services Used:

Amazon S3
Amazon S3 was used as the cloud storage layer for storing:
•	Raw landing datasets
•	Trusted datasets
•	Curated datasets
•	Machine learning datasets
•	Athena query results
________________________________________
AWS Glue
AWS Glue was used for:
•	ETL pipeline creation
•	Data transformation
•	Data sanitization
•	Schema management
•	Glue Data Catalog integration
•	PySpark job execution
Glue Studio Visual ETL with SQL Query nodes was used for implementing all transformations.
________________________________________
AWS Athena
Amazon Athena was used to:
•	Create external tables
•	Query raw and transformed datasets
•	Validate row counts
•	Verify ETL outputs
________________________________________
AWS IAM
AWS IAM roles and policies were configured to provide Glue and Athena with secure access to S3 resources.
________________________________________
# Dataset Description
The project uses three JSON datasets.
________________________________________
1. Customer Dataset
Source: STEDI fulfillment and website system
Fields
•	serialnumber
•	sharewithpublicasofdate
•	birthday
•	registrationdate
•	sharewithresearchasofdate
•	customername
•	email
•	lastupdatedate
•	phone
•	sharewithfriendsasofdate
Landing Table
customer_landing
Total Rows
956
________________________________________
2. Accelerometer Dataset
Source: Mobile application accelerometer readings
Fields
•	user
•	timestamp
•	x
•	y
•	z
Landing Table
accelerometer_landing
Total Rows
81273
________________________________________
3. Step Trainer Dataset
Source: STEDI IoT motion sensors
Fields

•	sensorreadingtime

•	serialnumber

•	distancefromobject

Landing Table
step_trainer_landing
Total Rows
28680
________________________________________
# ETL Pipeline
Landing Zone
The landing zone stores raw JSON data exactly as received from the source systems.
Landing Tables

•	customer_landing

•	accelerometer_landing

•	step_trainer_landing

Athena external tables were manually created for all landing datasets.
________________________________________
Trusted Zone
The trusted zone contains sanitized datasets filtered according to customer research consent policies.
________________________________________
customer_trusted
Purpose:
Store only customers who agreed to share their data for research purposes.
Transformation Logic
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
Output Rows
482
________________________________________
accelerometer_trusted
Purpose:
Store accelerometer readings only for customers who consented to research data sharing.
Transformation Logic
SELECT a.*
FROM accelerometer_landing a
INNER JOIN customer_trusted c
ON a.user = c.email
Output Rows
40981
________________________________________
step_trainer_trusted
Purpose:
Store step trainer readings only for trusted customers with accelerometer activity.
Transformation Logic
SELECT s.*
FROM step_trainer_landing s
INNER JOIN customers_curated c
ON s.serialnumber = c.serialnumber
Output Rows
14460
________________________________________
Curated Zone
The curated zone contains machine-learning-ready datasets.
________________________________________
customers_curated
Purpose:
Store customers who:
•	agreed to research sharing
•	have accelerometer data available
Transformation Logic
SELECT DISTINCT c.*
FROM customer_trusted c
INNER JOIN accelerometer_trusted a
ON c.email = a.user
Output Rows
482
________________________________________
machine_learning_curated
Purpose:
Create the final machine learning dataset by combining accelerometer and step trainer sensor data using matching timestamps.
Transformation Logic
SELECT
    a.*,
    s.distancefromobject
FROM accelerometer_trusted a
INNER JOIN step_trainer_trusted s
ON a.timestamp = s.sensorreadingtime
Output Rows
43681
________________________________________
Privacy and Data Governance
Customer privacy was a primary requirement for this project.
Only customers who explicitly consented to research data sharing were included in trusted and curated datasets.
The following condition was used throughout the ETL pipeline:
sharewithresearchasofdate IS NOT NULL
This ensures GDPR-style privacy compliance and proper handling of protected customer information.
________________________________________
Glue Jobs Implemented
The following AWS Glue jobs were created:

•	customer_landing_to_trusted.py

•	accelerometer_landing_to_trusted.py

•	customer_trusted_to_curated.py

•	step_trainer_trusted.py

•	machine_learning_curated.py

All Glue jobs were implemented using:

•	AWS Glue Studio

•	Visual ETL

•	SQL Query transformation nodes

•	PySpark

________________________________________
# Athena Validation
Athena queries were used after every ETL stage to validate row counts and data quality.

Final Row Counts

Table	                   Row Count
customer_landing	   956
accelerometer_landing	   81273
step_trainer_landing	   28680
customer_trusted	   482
accelerometer_trusted	   40981
customers_curated	   482
step_trainer_trusted	   14460
machine_learning_curated	43681
________________________________________
# S3 Folder Structure

customer/landing/
customer/trusted/
customer/curated/


accelerometer/landing/
accelerometer/trusted/


step_trainer/landing/
step_trainer/trusted/


machine_learning_curated/


athena-results/

________________________________________
# Project Outcome
A complete AWS-based data lakehouse solution was successfully implemented for STEDI Human Balance Analytics.
The final machine-learning-ready dataset was created by:

•	ingesting raw IoT and accelerometer data

•	filtering customer data using privacy rules

•	transforming datasets through ETL pipelines

•	validating outputs using Athena


The project demonstrates practical experience with:

•	cloud data engineering

•	AWS analytics services

•	ETL pipeline development

•	PySpark

•	data lakehouse architecture

•	privacy-aware data processing

________________________________________
