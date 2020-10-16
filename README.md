
# Project Data Pipelines with Airflow


## Table of Content
1. [Introduction](#Introduction)
2. [Project Structure](#Project_Structure)
3. [Requirements](#Requirements)
4. [Getting Started](Getting_Started)

## Introduction
The goal for this project is to set up an ETL pipeline between
S3 and AWS Redshift, creating a Data Warehouse for the company, Sparkify. Their data resides in S3, in a directory of JSON logs of user activity on the app, as well as a directory with JSON metadata on the songs from their streaming app.

To complete the ETL, custom operators is created to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

![Dag in Airflow](img/ETL.png)


## Project Structure

```
Project Data Pipelines with Airflow
├── README.md
│       
├── dags
│   ├── airflow_dag.py
│   └── create_tables.sql           
│
└── plugins            
    ├── __init__.py    
    │
    ├── helpers
    │   ├── __init__.py
    │   └── sql_queries.py
    │
    └── operators
        ├── __init__.py
        ├── data_quality.py
        ├── load_dimension.py
        ├── load_fact.py
        └── stage_redshift.py
```

## Requirements

* Apache Airflow
* AWS account
* Python3
* boto3
* psycopg2

## Getting Started
create cluster  
Connections in Airflow  
run airflow_dag.py in Airflow
