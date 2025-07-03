# File: dags/utils/data_transformers.py
"""
Data transformation utilities
"""

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def transform_data(df, transformations):
    """
    Apply common transformations to DataFrame
    
    Args:
        df: Input DataFrame
        transformations: Dict of transformation rules
        
    Example transformations:
        {
            'column_types': {'trade_date': 'datetime', 'quantity': 'float'},
            'new_columns': {'total_value': 'quantity * price'},
            'aggregations': {'sum': ['quantity'], 'mean': ['price']},
            'filters': {'quantity': '> 0'}
        }
    
    Returns:
        pandas.DataFrame: Transformed data
    """
    result_df = df.copy()
    
    # Change column types
    if 'column_types' in transformations:
        for col, dtype in transformations['column_types'].items():
            if col in result_df.columns:
                if dtype == 'datetime':
                    result_df[col] = pd.to_datetime(result_df[col])
                else:
                    result_df[col] = result_df[col].astype(dtype)
    
    # Add new columns
    if 'new_columns' in transformations:
        for col_name, formula in transformations['new_columns'].items():
            # Simple eval - be careful with security in production
            result_df[col_name] = result_df.eval(formula)
    
    # Apply aggregations
    if 'aggregations' in transformations:
        agg_dict = {}
        for agg_func, columns in transformations['aggregations'].items():
            for col in columns:
                if col in result_df.columns:
                    agg_dict[f"{col}_{agg_func}"] = result_df[col].agg(agg_func)
        
        # Return aggregated results as single row DataFrame
        if agg_dict:
            result_df = pd.DataFrame([agg_dict])
    
    # Apply filters
    if 'filters' in transformations:
        for col, condition in transformations['filters'].items():
            if col in result_df.columns:
                result_df = result_df.query(f"{col} {condition}")
    
    return result_df


def enrich_from_snowflake(df, snowflake_conn_id, lookup_config):
    """
    Enrich DataFrame with data from Snowflake
    
    Args:
        df: Input DataFrame
        snowflake_conn_id: Airflow connection ID for Snowflake
        lookup_config: Configuration for lookup
        
    Example lookup_config:
        {
            'table': 'REFERENCE.INSTRUMENTS',
            'join_on': {'instrument_id': 'INSTRUMENT_ID'},
            'select_columns': ['ISIN', 'INSTRUMENT_NAME', 'SECTOR']
        }
    
    Returns:
        pandas.DataFrame: Enriched data
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Get unique values for lookup
    lookup_column = list(lookup_config['join_on'].keys())[0]
    unique_values = df[lookup_column].unique().tolist()
    
    # Build lookup query
    placeholders = ','.join([f"'{val}'" for val in unique_values])
    snowflake_column = lookup_config['join_on'][lookup_column]
    
    select_cols = ', '.join([snowflake_column] + lookup_config['select_columns'])
    
    query = f"""
    SELECT {select_cols}
    FROM {lookup_config['table']}
    WHERE {snowflake_column} IN ({placeholders})
    """
    
    # Execute query and get results
    lookup_df = hook.get_pandas_df(query)
    
    # Merge with original DataFrame
    result_df = df.merge(
        lookup_df,
        left_on=lookup_column,
        right_on=snowflake_column,
        how='left'
    )
    
    return result_df