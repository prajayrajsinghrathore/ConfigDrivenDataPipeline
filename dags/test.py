from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'snowflake_connection_test',
    default_args=default_args,
    description='Test Snowflake connection and run basic queries',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['snowflake', 'test', 'connection'],
)

def test_snowflake_connection(**context):
    """
    Test Snowflake connection using Hook
    """
    try:
        # Initialize Snowflake Hook
        hook = SnowflakeHook(snowflake_conn_id='snowflake-default')
        
        # Test connection
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Execute a simple query
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        
        print(f"✅ Snowflake connection successful!")
        print(f"Snowflake Version: {result[0]}")
        
        cursor.close()
        conn.close()
        
        return "Connection test passed"
        
    except Exception as e:
        print(f"❌ Snowflake connection failed: {str(e)}")
        raise

# Task 1: Test connection using Python operator
test_connection_task = PythonOperator(
    task_id='test_snowflake_connection',
    python_callable=test_snowflake_connection,
    dag=dag,
)

# Task 2: Get current timestamp
get_timestamp_task = SnowflakeSqlApiOperator(
    task_id='get_current_timestamp',
    snowflake_conn_id='snowflake_default',
    sql="SELECT CURRENT_TIMESTAMP() as current_time;",
    dag=dag,
)

# Task 3: Get account information
get_account_info_task = SnowflakeSqlApiOperator(
    task_id='get_account_info',
    snowflake_conn_id='snowflake_default',
    sql="""
    SELECT 
        CURRENT_ACCOUNT() as account,
        CURRENT_REGION() as region,
        CURRENT_USER() as user_name,
        CURRENT_ROLE() as current_role,
        CURRENT_WAREHOUSE() as warehouse,
        CURRENT_DATABASE() as database,
        CURRENT_SCHEMA() as schema;
    """,
    dag=dag,
)

# Task 4: Show warehouses
show_warehouses_task = SnowflakeSqlApiOperator(
    task_id='show_warehouses',
    snowflake_conn_id='snowflake_default',
    sql="SHOW WAREHOUSES;",
    dag=dag,
)

# Task 5: Show databases
show_databases_task = SnowflakeSqlApiOperator(
    task_id='show_databases',
    snowflake_conn_id='snowflake_default',
    sql="SHOW DATABASES;",
    dag=dag,
)

# Task 6: Test simple math operation
test_math_task = SnowflakeSqlApiOperator(
    task_id='test_math_operation',
    snowflake_conn_id='snowflake_default',
    sql="""
    SELECT 
        1 + 1 as simple_math,
        ROUND(PI(), 4) as pi_value,
        SQRT(16) as square_root;
    """,
    dag=dag,
)

# Task 7: Success notification
success_task = BashOperator(
    task_id='success_notification',
    bash_command='echo "🎉 All Snowflake tests completed successfully!"',
    dag=dag,
)

# Define task dependencies
test_connection_task >> [get_timestamp_task, get_account_info_task]
get_account_info_task >> [show_warehouses_task, show_databases_task]
[get_timestamp_task, show_warehouses_task, show_databases_task] >> test_math_task
test_math_task >> success_task