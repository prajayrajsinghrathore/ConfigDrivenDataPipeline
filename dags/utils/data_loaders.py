# File: dags/utils/data_loaders.py
"""
Data loading utilities for various destinations
"""

import pandas as pd
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import json


def load_to_azure_data_lake(df, azure_conn_id, container, file_path, file_format='parquet'):
    """
    Load DataFrame to Azure Data Lake
    
    Args:
        df: DataFrame to load
        azure_conn_id: Airflow connection ID for Azure Data Lake
        container: Container name
        file_path: Path within container
        file_format: File format ('parquet', 'csv', 'json')
    """
    hook = AzureDataLakeHook(azure_data_lake_conn_id=azure_conn_id)
    
    # Add timestamp to filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"{file_path}_{timestamp}.{file_format}"
    
    if file_format == 'parquet':
        data = df.to_parquet()
    elif file_format == 'csv':
        data = df.to_csv(index=False)
    elif file_format == 'json':
        data = df.to_json(orient='records')
    else:
        raise ValueError(f"Unsupported format: {file_format}")
    
    hook.upload_file(
        local_path=None,
        remote_path=file_name,
        file_system_name=container,
        overwrite=True,
        data=data
    )
    
    return file_name


def load_to_azure_blob(df, azure_conn_id, container, blob_name, file_format='parquet'):
    """
    Load DataFrame to Azure Blob Storage
    
    Args:
        df: DataFrame to load
        azure_conn_id: Airflow connection ID for Azure Blob
        container: Container name
        blob_name: Blob name
        file_format: File format ('parquet', 'csv', 'json')
    """
    hook = WasbHook(wasb_conn_id=azure_conn_id)
    
    # Add timestamp to blob name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    full_blob_name = f"{blob_name}_{timestamp}.{file_format}"
    
    if file_format == 'parquet':
        data = df.to_parquet()
    elif file_format == 'csv':
        data = df.to_csv(index=False)
    elif file_format == 'json':
        data = df.to_json(orient='records')
    else:
        raise ValueError(f"Unsupported format: {file_format}")
    
    hook.load_string(
        string_data=data,
        container_name=container,
        blob_name=full_blob_name
    )
    
    return full_blob_name


def load_to_snowflake(df, snowflake_conn_id, table_name, schema='PUBLIC', database=None, if_exists='append'):
    """
    Load DataFrame to Snowflake table using SQL INSERT statements
    
    Args:
        df: DataFrame to load
        snowflake_conn_id: Airflow connection ID for Snowflake
        table_name: Target table name
        schema: Schema name
        database: Database name (optional)
        if_exists: What to do if table exists ('append', 'replace', 'fail')
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    if len(df) == 0:
        return "No data to load"
    
    # Full table name
    full_table_name = f"{schema}.{table_name}"
    if database:
        full_table_name = f"{database}.{full_table_name}"
    
    try:
        # Create table if it doesn't exist (simple approach)
        if if_exists == 'replace':
            # Drop table if it exists
            hook.run(f"DROP TABLE IF EXISTS {full_table_name}")
        
        # Get column names and create table SQL
        columns = df.columns.tolist()
        
        # Create table with VARCHAR columns (simple approach)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {', '.join([f'"{col}" VARCHAR' for col in columns])}
        )
        """
        hook.run(create_sql)
        
        # Insert data in batches
        batch_size = 1000
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            # Prepare INSERT statement
            values_list = []
            for _, row in batch_df.iterrows():
                # Convert each value to string and escape quotes
                row_values = []
                for val in row:
                    if pd.isna(val) or val is None:
                        row_values.append('NULL')
                    else:
                        # Escape single quotes and wrap in quotes
                        escaped_val = str(val).replace("'", "''")
                        row_values.append(f"'{escaped_val}'")
                values_list.append(f"({', '.join(row_values)})")
            
            # Build and execute INSERT statement
            insert_sql = f"""
            INSERT INTO {full_table_name} ({', '.join([f'"{col}"' for col in columns])})
            VALUES {', '.join(values_list)}
            """
            
            hook.run(insert_sql)
        
        return f"Loaded {total_rows} rows to {full_table_name}"
        
    except Exception as e:
        # Fallback: use pandas to_sql with direct connection
        try:
            engine = hook.get_sqlalchemy_engine()
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            engine.dispose()
            return f"Loaded {len(df)} rows to {schema}.{table_name} (fallback method)"
        except Exception as e2:
            raise Exception(f"Both methods failed. SQL method: {str(e)}, SQLAlchemy method: {str(e2)}")


def load_to_snowflake_stage(df, snowflake_conn_id, stage_name, file_name, file_format='csv'):
    """
    Load DataFrame to Snowflake stage using SQL
    
    Args:
        df: DataFrame to load
        snowflake_conn_id: Airflow connection ID for Snowflake
        stage_name: Stage name
        file_name: File name in stage
        file_format: File format ('csv', 'json')
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Convert DataFrame to string
    if file_format == 'csv':
        data = df.to_csv(index=False)
    elif file_format == 'json':
        data = df.to_json(orient='records')
    else:
        raise ValueError(f"Unsupported format: {file_format}")
    
    # Create a temporary file and upload to stage
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=f'.{file_format}') as tmp_file:
        tmp_file.write(data)
        tmp_file_path = tmp_file.name
    
    try:
        # Upload to stage using PUT command
        put_sql = f"PUT file://{tmp_file_path} @{stage_name}/{file_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        hook.run(put_sql)
        
        return f"Loaded to stage {stage_name}/{file_name}"
    finally:
        # Clean up temporary file
        os.unlink(tmp_file_path)