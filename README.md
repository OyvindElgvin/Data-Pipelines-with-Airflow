# Work in progress
# Project Data Pipelines with Airflow


## Table of Content
linker


## Introduction
The goal for this project is to set up an ETL pipeline between
S3 and AWS Redshift, creating a Data Warehouse for the company, Sparkify. Their data resides in S3, in a directory of JSON logs of user activity on the app, as well as a directory with JSON metadata on the songs from their streaming app.

To complete the ETL, custom operators is created to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

![Dag in Airflow](img/ETL.png)

'''
some code here
'''

## Requirements
list

## Getting Started
create cluster  
Connections in Airflow  
run airflow_dag.py in Airflow
