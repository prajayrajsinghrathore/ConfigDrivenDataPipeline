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
    Load DataFrame to Snowflake table
    
    Args:
        df: DataFrame to load
        snowflake_conn_id: Airflow connection ID for Snowflake
        table_name: Target table name
        schema: Schema name
        database: Database name (optional)
        if_exists: What to do if table exists ('append', 'replace', 'fail')
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Use pandas to_sql method with Snowflake connection
    with hook.get_sqlalchemy_engine() as engine:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
    
    return f"Loaded {len(df)} rows to {schema}.{table_name}"


def load_to_snowflake_stage(df, snowflake_conn_id, stage_name, file_name, file_format='csv'):
    """
    Load DataFrame to Snowflake stage
    
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
    
    # Upload to stage
    hook.run(f"PUT 'data:///{data}' @{stage_name}/{file_name}")
    
    return f"Loaded to stage {stage_name}/{file_name}"