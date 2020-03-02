# Data Pipeline with Airflow

The goal of this project is to build ETL pipeline that extracts data from Amazon S3, stages it in Amazon Redshift and transforming data into dimensional tables for analysis. The data pipeline workflow will be automated and monitored using Apache Airflow. The data is about song listening activity and song/artist information. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs. 

## Getting Started

Cloning the repository will get you a copy of the project up and running on your local machine for development and testing purposes. 

- `git@github.com:ypatankar/airflow-redshift-datapipeline.git`
- `https://github.com/ypatankar/airflow-redshift-datapipeline.git`

### Prerequisites

* python 3.7
* AWS account
* Amazon Redshift 4 node cluster and IAM role to let Redshift access S3 bucket
* Open an incoming TCP port in security group to access the cluster endpoint
* Apache Airflow setup

### Contents

* `create_tables.sql` : Create table sql queries used by the dag task
* `sql_queries.py` : Sql statements to load data in the fact and dimension tables
* `udac_example_dag.py` : Main file that orchestrates the Airflow dag and tasks
* `stage_redshift.py` : Custom Airflow operator to stage data from S3 to Redshift
* `load_fact.py` : Custom Airflow operator to load data into fact table in Redshift
* `load_dimension.py` : Custom Airflow operator to load data into fact table in Redshift
* `data_quality.py` : Custom Airflow operator to run quality checks on the data loaded in Redshift

### Database Design
![alt text](https://github.com/ypatankar/airflow-redshift-datapipeline/blob/master/airflowdag.png)

Table Name | Type
--- | ---
songplays | fact
songs | dimension
artists | dimension
users | dimension
times | dimension

### Deployment Steps
1. Copy the cloned files into the Airflow dag bag folder where Airflow is setup
3. Load Apache Airflow web UI and ensure that sparkify_dag shows in the list
4. When the above setup is ready, switch on the dag and the dag will execute hourly

