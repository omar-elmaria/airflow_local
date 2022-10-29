# airflow_local
This repo contains the DAGs that run on my local Airflow dev environment. I use the local environment to test my DAGs before deploying them to virtual machines via Kubernetes

# 1. Structure of the repo
The repo is divided into two folders ```dags``` and ```sql_queries```. The folder ```dags``` contains the Py scripts that create the DAGs. It also contains **two additional subfolders**, ```functions``` and ```groups```. The ```functions``` subfolder contains any Python **modules** or **callables** that I import into my DAG scripts. The ```groups``` subfolder contains ```TaskGroup``` modules that I occasionally use in some DAGs. TaskGroups is an Airflow feature that collates **multiple related tasks** together under one task name.

The root of the repo also contains ```Dockerfile``` and ```docker-compose.yaml``` files. These dictate how the Airflow image on my local machine is constructed (e.g., what **Python dependencies** and **Airflow configuration settings** should be used). If you want to know how to use these files to customize your Airflow instance, please refer to my [airflow_installation_instructions](https://github.com/omar-elmaria/airflow_installation_instructions) guide. The root also contains a requirements.txt file that shows the **installed Python modules** that I use in my **virtual environment**.

# 2. Existing DAGs
The repo currently has **four** DAGs:
- switchback_test_dag.py
- loved_brands_automation_dag.py
- homzmart_scraping_dag.py
- bigquery_operator_test_dag.py

The **first two** are related to projects that I led at Delivery Hero and are not reproducible on other machines because they use proprietary data sources. However, you can check them to see how to use various Airflow features such as ```TaskGroups```, ```PythonOperator```, ```EmailOperator```, and ```Bash Operator```.

The **third** DAG orchestrates the scripts found in this repo --> [python_scrapy_airflow_pipeline](https://github.com/omar-elmaria/python_scrapy_airflow_pipeline). This DAG runs scripts that crawl the website of an E-commerce company that sells furniture online.

The **fourth** DAG tests the ```BigQueryExecuteQueryOperator```, which is way of accessing data in Google Big Query **without** instantiating a BigQuery client as shown below
```
client = bigquery.Client(project="logistics-data-staging-flat")
bqstorage_client = bigquery_storage.BigQueryReadClient()
data_query = """SELECT * FROM `{PROJECT}.{DATASET}.{TABLE_NAME}`"""
df = client.query(query=data_query).result().to_dataframe(bqstorage_client=bqstorage_client)
```

# 3. Questions?
If you have any questions or wish to build a scraper for a particular use case, feel free to contact me on [LinkedIn](https://www.linkedin.com/in/omar-elmaria/)
