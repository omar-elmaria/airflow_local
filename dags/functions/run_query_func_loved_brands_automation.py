import re
from google.cloud import bigquery
from datetime import datetime

# Define functions to be used in the DAG
def run_query_func(**kwargs):
    # Define some inputs
    common_path = "/opt/airflow/loved_brands_automation/sql_queries"
    sessions_ntile_thr = 0.75
    orders_ntile_thr = 0.75
    cvr3_ntile_thr = 0.75
    suffix = kwargs["file_name"]

    # Instantiate a BQ client
    client = bigquery.Client(project="logistics-data-staging-flat")

    # Read the SQL file
    with open(f"{common_path}/{suffix}", mode="r") as f:
        sql_script = f.read()

    # Add query parameters if they exist
    if re.findall("step_[0-9]+.[0-9]|step_[0-9]+", suffix)[0] in ["step_10", "step_11"]:
        sql_script = re.sub("sessions_ntile_thr", str(sessions_ntile_thr), sql_script)
        sql_script = re.sub("orders_ntile_thr", str(orders_ntile_thr), sql_script)
        sql_script = re.sub("cvr3_ntile_thr", str(cvr3_ntile_thr), sql_script)
    else:
        pass

    # Run the SQL script
    client.query(sql_script).result()

    # Print a success message
    print(
        "The SQL script {} was executed successfully at {} \n".format(
            suffix, datetime.now()
        )
    )