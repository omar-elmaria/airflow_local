# airflow_local
This repo contains the DAGs that run on my local Airflow dev environment. I use the local environment to test my DAGs before deploying them to virtual machines via Kubernetes

# Structure of the repo
The repo is divided into two folders ```dags``` and ```sql_queries```. The folder ```dags``` contains the Py scripts that create the DAGs. It also contains **two additional subfolders**, ```functions``` and ```groups```. The ```functions``` subfolder contains any Python **modules** or **callables** that I import into my DAG scripts. The ```groups``` subfolder contains ```TaskGroup``` modules that I occasionally use in some DAGs. TaskGroups is an Airflow feature that collates **multiple related tasks** together under one task name.

The root of the repo also contains ```Dockerfile``` and ```docker-compose.yaml``` files. These dictate how the Airflow image on my local machine is constructed (e.g., what **Python dependencies** and **Airflow configuration settings** should be used). If you want to know how to use these files to customize your Airflow instance, please refer to my [airflow_installation_instructions](https://github.com/omar-elmaria/airflow_installation_instructions) guide. The root also contains a requirements.txt file that shows the **installed Python modules** that I use in my **virtual environment**.

# Existing DAGs
