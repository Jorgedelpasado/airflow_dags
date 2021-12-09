# Pre-work

## Infrastructure
This solution is mainly based on Apache Airflow, in order to run Airflow, a kubernetes cluster is created in GCP using **Terraform**, steps to do this are taken from [terraform-gcp-gke-airflow](https://github.com/wizelineacademy/data-bootcamp-terraforms/tree/master/gcp).

For the storage layers, three GCS buckets will be used, two of them are for **raw** and **staging** layer, last one is used to store the pyspark code that is submitted to a dataproc cluster, these buckets can be created using the Terraform code in [Terraform blocks](https://github.com/Jorgedelpasado/terraform_blocks/tree/master/terraform_buckets).

A service account must be created to allow Airflow to make the changes needed in the GCP project, the roles this account needs are:

* BigQuery User
* Cloud SQL Editor
* Dataproc Administrator
* Service Account User
* Storage Object Admin

Two connections need to be created in Airflow, **google_cloud_default** using the new service account and **postgres_default** for connecting to the database created with Terraform.

Finally the files **user_purchase.csv** and **movie_review.csv** are uploaded manually from local to the raw layer.

## Initial state
As the problem description mentions, one of the data sources is a PostgreSQL table, so the first thing to do is create this table and copy data into it. This is done with DAG `0_gcs_to_postgres.py`, and what it does is:

* Checks if table exists, if it doesn't then creates it usig the schema specified in `sql/create_schema.sql`.
* Once table is created, it copies the **user_purchase.csv** file into it.

# Solution

## Step 1
The first step will be to get the data from the PostgreSQL table, this is done by `1_postgres_to_gcs.py`. It connects to the database and dumps the data into a temporary file, this file is uploaded to the staging layer.

## Step 2
Next step is to read and transform the data from **movie_review.csv** and save it in the staging layer.

First, `pyspark/tokenizer.py` needs to be uploaded to the code bucket, since this is where the jobs are taking the code from.

`2_submit_tokenizer.py` creates an efimeral dataproc cluster and submits the pyspark job, it reads the **movie_review.csv** into a data frame and transforms the data to get a new data frame where positive reviews are filtered.

*It is worth to mention that Terraform was not used to create the Dataproc cluster since Airflow has operators that are easier to implement in this case, nevertheless Terraform code for this purpose can be found in* [Terraform Dataproc](https://github.com/Jorgedelpasado/terraform_blocks/tree/master/terraform_dataproc).

What this DAG does is, first split the string containing the review into a list of individual words using the **RegexTokenizer** from **pyspark.ml.feature**, then if this list has the word `good`, it is considered a positive review and gets the value `1` and `0` otherwise.

Here's a sample of the data frame:

|  cid  | positive_review |
|:-----:|:---------------:|
| 16579 |        1        |
| 14841 |        0        |
| 18085 |        0        |
| 16365 |        1        |
| 17912 |        0        |

This data frame is written in a parquet file in the staging layer.

## Step 3

At this point there are two files in the staging layer, a CSV file with the data dumped from the PostgreSQL table and a parquet file from **Step 2**, now is time to use them to get the data for the expected output.

It is necesary to upload the code for the job in the code bucket, this time the file needed is `pyspark/user_behavior.py`

`3_submit_user_behavior.py` creates a dataproc cluster and submits the job, this job consists on:

* Reading both files in staging layers into separate data frames, first one uses **user_purchase.csv** to calculate the total amount spent by every customer.
* Second dataframe, counts the total reviews written by the customers and how many of those reviews are positives.
* Next, the two dataframes with the calculated data are joined by Customer ID and a new column is created for storing a timestamp.
* The data frame is written in a parquet file in the staging layer.

Final data frame sample:

| customer_id | amount_spent       | review_count | review_score | insert_date              |
|-------------|--------------------|--------------|--------------|--------------------------|
| 12662       | 3817.080005645752  | 118          | 46           | 2021-12-08T04:55:07.365Z |
| 12704       | 2220.839982509613  |              |              | 2021-12-08T04:55:07.365Z |
| 12705       | 6814.2399978637695 | 115          | 38           | 2021-12-08T04:55:07.365Z |
| 12708       | 2616.3200166225433 | 118          | 37           | 2021-12-08T04:55:07.365Z |
| 12709       | 9294.100022554398  | 111          | 40           | 2021-12-08T04:55:07.365Z |

Once job is finished, two tasks are simultaneously run, one triggers the next DAG to upload the parquet file to BigQuery if job succeeded, the other task destroys the cluster, even if job fails.

## Step 4

Finally, since the data won't be further modified it can be stored in BigQuery, this is done by the last DAG.

`4_staging_to_bq.py` as mentioned before is triggered once the pyspark job correctly wrote in the staging layer, it first creates an empty dataset in BigQuery, and then writes the parquet file in a table that creates.

With the data stored in BigQuery it can be analyzed or used in dashboards, which was the goal for the company described in the problem.