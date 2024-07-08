import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "Astro",
}

def create_subdag(parent_dag_id, child_dag_id, table_config):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        default_args=default_args,
        description=f"ETL workflow for {table_config['process_name']}",
        schedule_interval=None,
        start_date=days_ago(2),
        catchup=False,
    )

    # This is a placeholder for your actual ETL process.
    # Replace this with your ETL logic.
    etl_task = BashOperator(
        task_id='run_etl',
        bash_command='echo "Running ETL process for {{ params.process_name }}"',
        params={'process_name': table_config['process_name']},
        dag=dag_subdag,
    )

    return dag_subdag

# Parent DAG
parent_dag_id = "dmart_era_customized_reporting_a606_load"
parent_dag = DAG(
    dag_id=parent_dag_id,
    default_args=default_args,
    description="A DAG to move data from Hive to Snowflake for multiple tables related to A606",
    schedule_interval="0 0 30 * *",
    start_date=days_ago(2),
    catchup=False,
)

# Table configurations
tables_config = {
    'table1': {
        'process_group': 'era', 'process_name': 'istock_credit_expiry', 'alert_name': 'snowflake_dmart_era_customized_reporting.expiry_by_acquiry', 'tag': '_dmart_era_customized_reporting.expiry_by_acquiry', 
        'prod_schema': 'dmart_era_customized_reporting', 'prod_table': 'expiry_by_acquiry', 'hql_scripts': ['/etl/era/finance_projects/etl/expiry_by_acquiry.sql']
    },
    'table2': {
        'process_group': 'era', 'process_name': 'istock_average_time_to_expire', 'alert_name': 'snowflake_dmart_era_customized_reporting.istock_Average_Tm_Expire', 'tag': 'dmart_era_customized_reporting.istock_Average_Tm_Expire', 
        'prod_schema': 'dmart_era_customized_reporting', 'prod_table': 'istock_Average_Tm_Expire', 'hql_scripts': ['/etl/era/finance_projects/etl/istock_Average_Tm_Expire.sql']
    },
    'table3': {
        'process_group': 'era', 'process_name': 'istock_booked_revenue', 'alert_name': 'snowflake_dmart_era_customized_reporting.booked_revenue', 'tag': 'dmart_era_customized_reporting.booked_revenue', 
        'prod_schema': 'dmart_era_customized_reporting', 'prod_table': 'booked_revenue', 'hql_scripts': ['/etl/era/finance_projects/etl/istoct_credit_booked_revenue.sql']
    },
    
}

# Create subdags and tasks for each table
for table, config in tables_config.items():
    subdag_task = SubDagOperator(
        task_id=f"etl_{config['process_name']}_subdag",
        subdag=create_subdag(parent_dag_id, f"etl_{config['process_name']}_subdag", config),
        dag=parent_dag,
    )

    # This is a placeholder for your actual ETL process.
    # Replace this with your actual steps to process the data.
    gsync_step = BashOperator(
        task_id=f"gsync_{config['process_name']}",
        bash_command='echo "Running gsync step for {{ params.process_name }}"',
        params={"process_name": config["process_name"]},
        dag=parent_dag,
    )

    gmerge_step = BashOperator(
        task_id=f"gmerge_{config['process_name']}",
        bash_command='echo "Running gmerge step for {{ params.process_name }}"',
        params={"process_name": config["process_name"]},
        dag=parent_dag,
    )

    subdag_task >> gsync_step >> gmerge_step
