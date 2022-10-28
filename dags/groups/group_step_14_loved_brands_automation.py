from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from functions.run_query_func_loved_brands_automation import run_query_func
from functions.linear_reg_func import linear_reg_func

def step_14_cvr3_per_df_tier_vendor_level():
    with TaskGroup("step_14_cvr3_per_df_tier_vendor_level", tooltip="step_14_cvr3_per_df_tier_vendor_level") as group:
        step_14_1_cvr3_per_df_tier_per_vendor = PythonOperator(
            task_id="step_14.1_cvr3_per_df_tier_per_vendor",
            python_callable=run_query_func,
            op_kwargs={"file_name": "step_14.1_cvr3_per_df_tier_per_vendor.sql"},
        )

        step_14_2_cvr3_at_min_df_per_vendor = PythonOperator(
            task_id="step_14.2_cvr3_at_min_df_per_vendor",
            python_callable=run_query_func,
            op_kwargs={"file_name": "step_14.2_cvr3_at_min_df_per_vendor.sql"},
        )

        step_14_3_pct_drop_vendor_cvr3_from_base = PythonOperator(
            task_id="step_14.3_pct_drop_vendor_cvr3_from_base",
            python_callable=run_query_func,
            op_kwargs={"file_name": "step_14.3_pct_drop_vendor_cvr3_from_base.sql"},
        )
        
        step_14_4_linear_reg = PythonOperator(
            task_id="step_14.4_linear_reg",
            python_callable=linear_reg_func(granularity="vendor")
        )
        
        step_14_1_cvr3_per_df_tier_per_vendor >> step_14_2_cvr3_at_min_df_per_vendor >> step_14_3_pct_drop_vendor_cvr3_from_base >> step_14_4_linear_reg
        return group