from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import datetime as dt
from groups.group_step_14_loved_brands_automation import step_14_cvr3_per_df_tier_vendor_level
from groups.group_step_15_loved_brands_automation import step_15_cvr3_per_df_tier_asa_level
from functions.run_query_func_loved_brands_automation import run_query_func
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/application_default_credentials.json"

default_args = {
    "owner": "oelmaria",
    "email": "omar.elmaria@deliveryhero.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": True,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=0.05), # 3 seconds
    "start_date": dt.datetime(2021, 1, 1)
}

doc_md = """
#### Orchestrate the loved brands curation logic and push the results to a S3 bucket
"""

with DAG(
    dag_id="loved_brands_dag", 
    default_args=default_args,
    schedule_interval="0 4 3 * *",  # At 04:00 am on day-of-month 3
    catchup=False,
    tags=["pricing", "loved_brands"],
) as dag:

    dag.doc_md = doc_md

    step_0_dummy = DummyOperator(
        task_id="step_0_dummy"
    )

    step_1_active_entities = PythonOperator(
        task_id="step_1_active_entities",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_1_active_entities.sql"},
    )

    step_2_geo_data = PythonOperator(
        task_id="step_2_geo_data",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_2_geo_data.sql"},
    )

    step_3_1_asa_setups = PythonOperator(
        task_id="step_3.1_asa_setups",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_3.1_asa_setups.sql"},
    )

    step_3_2_identify_clustered_asa = PythonOperator(
        task_id="step_3.2_identify_clustered_asa",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_3.2_identify_clustered_asa.sql"},
    )

    step_3_3_add_master_asa_id = PythonOperator(
        task_id="step_3.3_add_master_asa_id",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_3.3_add_master_asa_id.sql"},
    )

    step_4_vendors_per_asa = PythonOperator(
        task_id="step_4_vendors_per_asa",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_4_vendors_per_asa.sql"},
    )

    step_5_schemes_per_asa = PythonOperator(
        task_id="step_5_schemes_per_asa",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_5_schemes_per_asa.sql"},
    )

    step_6_df_tiers_per_scheme = PythonOperator(
        task_id="step_6_df_tiers_per_scheme",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_6_df_tiers_per_scheme.sql"},
    )

    step_7_df_tiers_per_asa = PythonOperator(
        task_id="step_7_df_tiers_per_asa",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_7_df_tiers_per_asa.sql"},
    )

    step_8_ga_session_data = PythonOperator(
        task_id="step_8_ga_session_data",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_8_ga_session_data.sql"},
    )

    step_9_1_vendor_order_data_for_screening = PythonOperator(
        task_id="step_9.1_vendor_order_data_for_screening",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_9.1_vendor_order_data_for_screening.sql"},
    )

    step_9_2_asa_order_data_for_impact_analysis = PythonOperator(
        task_id="step_9.2_asa_order_data_for_impact_analysis",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_9.2_asa_order_data_for_impact_analysis.sql"},
    )

    step_10_all_metrics_vendor_screening = PythonOperator(
        task_id="step_10_all_metrics_vendor_screening",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_10_all_metrics_vendor_screening.sql"},
    )

    step_11_filtering_for_vendors_by_pct_ranks = PythonOperator(
        task_id="step_11_filtering_for_vendors_by_pct_ranks",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_11_filtering_for_vendors_by_pct_ranks.sql"},
    )

    step_12_dps_logs_data = PythonOperator(
        task_id="step_12_dps_logs_data",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_12_dps_logs_data.sql"},
    )

    step_13_join_dps_logs_and_ga_sessions = PythonOperator(
        task_id="step_13_join_dps_logs_and_ga_sessions",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_13_join_dps_logs_and_ga_sessions.sql"},
    )

    step_14 = step_14_cvr3_per_df_tier_vendor_level()
    
    step_15 = step_15_cvr3_per_df_tier_asa_level()
    
    step_15_4_append_asa_tbl_to_vendor_tbl = PythonOperator(
        task_id="step_15.4_append_asa_tbl_to_vendor_tbl",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_15.4_append_asa_tbl_to_vendor_tbl.sql"},
    )
    
    step_16_final_vendor_list_temp = PythonOperator(
        task_id="step_16_final_vendor_list_temp",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_16_final_vendor_list_temp.sql"},
    )
    
    step_17_insert_new_records_to_final_tbl = PythonOperator(
        task_id="step_17_insert_new_records_to_final_tbl",
        python_callable=run_query_func,
        op_kwargs={"file_name": "step_17_insert_new_records_to_final_tbl.sql"},
    )

    (
        step_0_dummy
        >> step_1_active_entities
        >> [step_2_geo_data, step_3_1_asa_setups]
        >> step_3_2_identify_clustered_asa
        >> step_3_3_add_master_asa_id
        >> [step_4_vendors_per_asa, step_5_schemes_per_asa]
        >> step_6_df_tiers_per_scheme
        >> step_7_df_tiers_per_asa
        >> [step_8_ga_session_data, step_9_1_vendor_order_data_for_screening, step_9_2_asa_order_data_for_impact_analysis]
        >> step_10_all_metrics_vendor_screening
        >> [step_11_filtering_for_vendors_by_pct_ranks, step_12_dps_logs_data]
        >> step_13_join_dps_logs_and_ga_sessions
        >> [step_14, step_15]
        >> step_15_4_append_asa_tbl_to_vendor_tbl
        >> step_16_final_vendor_list_temp
        >> step_17_insert_new_records_to_final_tbl
    )
