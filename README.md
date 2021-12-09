# Wizeline Data Engineering Bootcamp - Capstone project.
## Problem Description:

Assume that you work for a user behavior analytics company that collects user data and creates user profiles. You must build a data pipeline to populate the **user_behavior_metric** table, this is an OLAP table. The data from **user_behavior_metric** is useful for analysts and tools like dashboard software.

The table **user_behavior_metric** takes information from:
* A PostgreSQL table named **user_purchase**.
* Daily data by an external vendor in a CSV file named **movie_review.csv** that populates the **classified_movie_review** table.

## Expected Outcome:

The goal is to create an end-to-end solution; this problem reflects a real-world scenario where a Data Engineer must configure the environment and resources, integrate the data, and load data from external sources where necessary. When the data is ready, transforming it to reflect business rules and deliver knowledge in the form of insights.

## Solution

This repository contains the code of the Airflow DAGs used to build one approach to the end-to-end solution necesary to solve the problem.

Inside folder **dags/** the files are enumerated in the execution order they should be run, more details on how they work is specified in the respective README, folder **sql/** contains the code for a query executed by one of the DAGs, finally the **pyspark/** folder has the code of **pyspark** jobs to be submitted and executed in a dataproc cluster.