from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/airflow/py_scripts/application_default_credentials.json"

default_args = {
    "owner": "oelmaria",
    "email": ["omar.elmaria@deliveryhero.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": True,
    "start_date": datetime(2017, 3, 20)
}

doc_md = """
#### Orchestrate the loved brands curation logic and push the results to a S3 bucket
"""

with DAG(
    "loved_brands_test_dag",
    schedule_interval="0 4 3 * *",  # At 04:00 on day-of-month 3
    default_args=default_args,
    catchup=False,
) as dag:

    dag.doc_md = doc_md

    step_0_dummy = DummyOperator(task_id="step_0_dummy")

    step_1_active_entities = BigQueryExecuteQueryOperator(
        task_id="step_1_active_entities",
        sql="""
        WITH dps AS (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`)

        SELECT
            ent.region,
            p.entity_id,
            LOWER(ent.country_iso) AS country_code,
            ent.country_name
        FROM `fulfillment-dwh-production.cl.entities` AS ent
        LEFT JOIN UNNEST(platforms) AS p
        INNER JOIN dps ON p.entity_id = dps.entity_id
        WHERE TRUE
            AND p.entity_id NOT LIKE "ODR%" -- Eliminate entities starting with ODR (on-demand riders)
            AND p.entity_id NOT LIKE "DN_%" -- Eliminate entities starting with DN_ as they are not part of DPS
            AND p.entity_id NOT IN ("FP_DE", "FP_JP", "BM_VN", "BM_KR") -- Eliminate irrelevant entity_ids in APAC
            AND p.entity_id NOT IN ("TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH") -- Eliminate irrelevant entity_ids in MENA
            AND p.entity_id NOT IN ("TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH") -- Eliminate irrelevant entity_ids in Europe
            AND p.entity_id NOT IN ("CD_CO") -- Eliminate irrelevant entity_ids in LATAM
        ;
        """,
        destination_dataset_table=f"dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code",
        write_disposition="WRITE_TRUNCATE",
        create_disposition = "CREATE_IF_NEEDED",
        gcp_conn_id="bigquery_default",
        use_legacy_sql=False
    )

    (
        step_0_dummy
        >> step_1_active_entities
    )
