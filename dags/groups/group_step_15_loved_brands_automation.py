from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

def step_15_cvr3_per_df_tier_asa_level():
    from functions.run_query_func_loved_brands_automation import run_query_func
    from functions.linear_reg_func import linear_reg_func
    
    with TaskGroup("step_15_cvr3_per_df_tier_asa_level", tooltip="step_15_cvr3_per_df_tier_asa_level") as group:
        step_15_1_cvr3_per_df_tier_per_asa = PythonOperator(
            task_id="step_15.1_cvr3_per_df_tier_per_asa",
            python_callable=run_query_func,
            op_kwargs={"file_name": "step_15.1_cvr3_per_df_tier_per_asa.sql"},
        )
    
        step_15_2_cvr3_at_min_df_per_asa = PythonOperator(
            task_id="step_15.2_cvr3_at_min_df_per_asa",
            python_callable=run_query_func,
            op_kwargs={"file_name": "step_15.2_cvr3_at_min_df_per_asa.sql"},
        )
        
        step_15_3_pct_drop_asa_cvr3_from_base = PythonOperator(
            task_id="step_15.3_pct_drop_asa_cvr3_from_base",
            python_callable=run_query_func,
            op_kwargs={"file_name": "step_15.3_pct_drop_asa_cvr3_from_base.sql"},
        )

        step_15_4_linear_reg = PythonOperator(
            task_id="step_15.4_linear_reg",
            python_callable=linear_reg_func(granularity="asa")
        )
        
        step_15_1_cvr3_per_df_tier_per_asa >> step_15_2_cvr3_at_min_df_per_asa >> step_15_3_pct_drop_asa_cvr3_from_base >> step_15_4_linear_reg
        return group