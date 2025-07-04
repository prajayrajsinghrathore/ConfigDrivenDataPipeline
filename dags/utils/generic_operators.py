# File: dags/utils/generic_operators.py
"""
Generic operators that can handle any data source type
"""

import pandas as pd
import numpy as np
from airflow.models import BaseOperator
from airflow.utils.context import Context
from typing import Dict, Any, Optional
import logging

from utils.data_fetchers import fetch_http_data, fetch_sftp_data
from utils.data_transformers import transform_data, enrich_from_snowflake
from utils.data_loaders import load_to_snowflake, load_to_snowflake_stage, load_to_azure_data_lake, load_to_azure_blob

logger = logging.getLogger(__name__)

class GenericDataIngestionOperator(BaseOperator):
    """
    Generic operator that can fetch data from any configured source
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.data_source_config = config['data_source']
        
    def execute(self, context: Context) -> pd.DataFrame:
        """Execute data ingestion based on configuration"""
        
        source_type = self.data_source_config['type']
        
        if source_type == 'rest_api':
            return self._fetch_from_api()
        elif source_type == 'sftp':
            return self._fetch_from_sftp()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _fetch_from_api(self) -> pd.DataFrame:
        """Fetch data from REST API"""
        endpoint = self.data_source_config['endpoint']
        auth_config = self.data_source_config.get('authentication', {})
        
        # Extract API key from authentication config
        api_key = None
        if auth_config.get('type') in ['bearer_token', 'api_key']:
            # In production, you'd get this from Airflow Variables or Connections
            api_key = auth_config.get('credentials')
        # For 'none' or 'oauth' types, api_key remains None
        
        # Get request configuration
        request_config = self.data_source_config.get('request_config', {})
        headers = request_config.get('headers', {})
        params = request_config.get('params', {})
        
        logger.info(f"Fetching data from API: {endpoint}")
        
        df = fetch_http_data(
            url=endpoint,
            api_key=api_key,
            headers=headers,
            params=params,
            format='json'
        )
        
        logger.info(f"Fetched {len(df)} rows from API")
        return df
    
    def _fetch_from_sftp(self) -> pd.DataFrame:
        """Fetch data from SFTP"""
        connection_id = self.data_source_config['connection_id']
        remote_path = self.data_source_config['remote_path']
        file_format = self.data_source_config.get('file_format', 'csv')
        
        logger.info(f"Fetching data from SFTP: {remote_path}")
        
        df = fetch_sftp_data(
            sftp_conn_id=connection_id,
            remote_path=remote_path,
            file_format=file_format
        )
        
        logger.info(f"Fetched {len(df)} rows from SFTP")
        return df

class GenericDataTransformationOperator(BaseOperator):
    """
    Generic operator that applies transformations based on configuration
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        input_data: Optional[pd.DataFrame] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.input_data = input_data
        
    def execute(self, context: Context) -> pd.DataFrame:
        """Execute transformations based on configuration"""
        
        # Get data from previous task if not provided
        if self.input_data is None:
            # Pull data from previous task using XCom
            self.input_data = context['task_instance'].xcom_pull(task_ids='ingest_data')
        
        df = self.input_data.copy()
        
        # Apply validation
        if 'validation_rules' in self.config:
            df = self._apply_validation(df)
        
        # Apply transformations
        if 'transformation' in self.config:
            df = self._apply_transformations(df)
        
        # Apply enrichment
        if 'enrichment' in self.config:
            df = self._apply_enrichment(df)
        
        logger.info(f"Transformation complete. Result: {len(df)} rows")
        
        # Convert to JSON-serializable format for XCom
        if len(df) > 0:
            # Convert DataFrame to list of dicts with safe serialization
            result = []
            for _, row in df.iterrows():
                record = {}
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        record[col] = None
                    elif isinstance(value, (list, dict)):
                        record[col] = value
                    else:
                        record[col] = str(value)  # Convert everything else to string for safety
                result.append(record)
            return result
        else:
            return []
    
    def _apply_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply validation rules from configuration"""
        validation_rules = self.config['validation_rules']
        
        for rule in validation_rules:
            field = rule['field']
            rule_type = rule['type']
            
            if field not in df.columns:
                if rule.get('required', False):
                    raise ValueError(f"Required field '{field}' not found in data")
                continue
            
            # Apply validation based on type
            if rule_type == 'numeric':
                if 'range' in rule:
                    min_val, max_val = rule['range']
                    if min_val is not None:
                        df = df[df[field] >= min_val]
                    if max_val is not None:
                        df = df[df[field] <= max_val]
            
            elif rule_type == 'datetime':
                # Convert to datetime if not already
                if not pd.api.types.is_datetime64_any_dtype(df[field]):
                    df[field] = pd.to_datetime(df[field])
            
            elif rule_type == 'string':
                if 'max_length' in rule:
                    max_len = rule['max_length']
                    df = df[df[field].str.len() <= max_len]
        
        return df
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations from configuration"""
        transformation_config = self.config['transformation']
        
        # Use a simpler approach to avoid pandas serialization issues
        result_data = []
        
        for _, row in df.iterrows():
            # Convert row to dict
            record = row.to_dict()
            
            # Apply column type changes
            if 'column_types' in transformation_config:
                for col, dtype in transformation_config['column_types'].items():
                    if col in record:
                        try:
                            if dtype == 'datetime':
                                record[col] = pd.to_datetime(record[col]).strftime('%Y-%m-%d %H:%M:%S')
                            elif dtype == 'float':
                                record[col] = float(record[col])
                            elif dtype == 'int':
                                record[col] = int(record[col])
                            else:
                                record[col] = str(record[col])
                        except:
                            record[col] = str(record[col])
            
            # Apply new columns
            if 'new_columns' in transformation_config:
                for col_name, formula in transformation_config['new_columns'].items():
                    try:
                        if '.str.upper()' in formula:
                            source_field = formula.split('.')[0]
                            if source_field in record:
                                record[col_name] = str(record[source_field]).upper()
                            else:
                                record[col_name] = 'UNKNOWN'
                        elif 'pd.Timestamp.now()' in formula:
                            record[col_name] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                        elif ' * ' in formula:
                            # Handle multiplication like "quantity * price"
                            parts = formula.split(' * ')
                            if len(parts) == 2 and parts[0].strip() in record and parts[1].strip() in record:
                                val1 = float(record[parts[0].strip()] or 0)
                                val2 = float(record[parts[1].strip()] or 0)
                                record[col_name] = val1 * val2
                            else:
                                record[col_name] = str(formula)
                        else:
                            record[col_name] = str(formula)
                    except Exception as e:
                        logger.warning(f"Error creating column {col_name}: {e}")
                        record[col_name] = f"ERROR: {str(e)}"
            
            # Apply filters
            include_record = True
            if 'filters' in transformation_config:
                for col, condition in transformation_config['filters'].items():
                    if col in record:
                        try:
                            value = float(record[col])
                            if '>' in condition:
                                threshold = float(condition.replace('>', '').strip())
                                if not (value > threshold):
                                    include_record = False
                                    break
                            elif '<' in condition:
                                threshold = float(condition.replace('<', '').strip())
                                if not (value < threshold):
                                    include_record = False
                                    break
                            elif '!=' in condition:
                                threshold = float(condition.replace('!=', '').strip())
                                if not (value != threshold):
                                    include_record = False
                                    break
                        except:
                            pass  # Skip filter if can't parse
            
            if include_record:
                result_data.append(record)
        
        return pd.DataFrame(result_data) if result_data else pd.DataFrame()
    
    def _apply_enrichment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply enrichment from configuration"""
        enrichment_configs = self.config['enrichment']
        
        for enrichment in enrichment_configs:
            if enrichment['type'] == 'reference_lookup':
                lookup_config = {
                    'table': enrichment['lookup_table'],
                    'join_on': {enrichment['source_field']: enrichment['lookup_field']},
                    'select_columns': enrichment['target_fields']
                }
                
                # Use default Snowflake connection
                df = enrich_from_snowflake(df, 'snowflake_default', lookup_config)
        
        return df

class GenericDataLoadOperator(BaseOperator):
    """
    Generic operator that loads data to configured destinations
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        input_data: Optional[pd.DataFrame] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.input_data = input_data
        
    def execute(self, context: Context) -> str:
        """Execute data loading based on configuration"""
        
        # Get data from previous task if not provided
        if self.input_data is None:
            self.input_data = context['task_instance'].xcom_pull(task_ids='transform_data')
        
        # Convert back to DataFrame if it's a list of dicts
        if isinstance(self.input_data, list):
            df = pd.DataFrame(self.input_data)
        else:
            df = self.input_data
        destination_config = self.config['destination']
        
        results = []
        
        # Load to primary destination
        if 'primary' in destination_config:
            result = self._load_to_destination(df, destination_config['primary'])
            results.append(f"Primary: {result}")
        
        # Load to backup destination
        if 'backup' in destination_config:
            result = self._load_to_destination(df, destination_config['backup'])
            results.append(f"Backup: {result}")
        
        # Load to archive destination
        if 'archive' in destination_config:
            result = self._load_to_destination(df, destination_config['archive'])
            results.append(f"Archive: {result}")
        
        return " | ".join(results)
    
    def _load_to_destination(self, df: pd.DataFrame, dest_config: Dict[str, Any]) -> str:
        """Load data to a specific destination"""
        dest_type = dest_config['type']
        
        if dest_type == 'snowflake_table':
            table = dest_config['table']
            mode = dest_config.get('mode', 'append')
            
            return load_to_snowflake(
                df=df,
                snowflake_conn_id='snowflake-default',
                table_name=table.split('.')[-1],
                schema=table.split('.')[-2] if '.' in table else 'PUBLIC',
                if_exists=mode
            )
        
        elif dest_type == 'snowflake_stage':
            mode = dest_config.get('mode', 'append')
            
            return load_to_snowflake_stage(
                df=df,
                snowflake_conn_id='snowflake-default',
                stage_name='FINNHUB_STAGE',
                file_name='finnhub_test_file.csv'
            )
        
        elif dest_type == 'azure_data_lake':
            container = dest_config['container']
            path = dest_config['path']
            file_format = dest_config.get('format', 'parquet')
            
            return load_to_azure_data_lake(
                df=df,
                azure_conn_id='azure_data_lake_default',
                container=container,
                file_path=path,
                file_format=file_format
            )
        
        elif dest_type == 'azure_blob':
            container = dest_config['container']
            path = dest_config['path']
            file_format = dest_config.get('format', 'parquet')
            
            return load_to_azure_blob(
                df=df,
                azure_conn_id='azure_blob_default',
                container=container,
                blob_name=path,
                file_format=file_format
            )
        
        elif dest_type == 'local_file':
            return self._load_to_local_file(df, dest_config)
        
        elif dest_type == 'print_logs':
            return self._load_to_logs(df, dest_config)
        
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
    
    def _load_to_local_file(self, df: pd.DataFrame, dest_config: Dict[str, Any]) -> str:
        """Save DataFrame to local temporary file"""
        import os
        from datetime import datetime
        
        # Create temp directory if it doesn't exist
        temp_dir = dest_config.get('path', '/tmp/airflow_output')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = dest_config.get('filename', 'data_output')
        file_format = dest_config.get('format', 'csv')
        
        filepath = os.path.join(temp_dir, f"{filename}_{timestamp}.{file_format}")
        
        # Save file based on format
        if file_format == 'csv':
            df.to_csv(filepath, index=False)
        elif file_format == 'json':
            df.to_json(filepath, orient='records', indent=2)
        elif file_format == 'parquet':
            df.to_parquet(filepath, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        logger.info(f"Saved {len(df)} rows to {filepath}")
        return f"Saved to {filepath}"
    
    def _load_to_logs(self, df: pd.DataFrame, dest_config: Dict[str, Any]) -> str:
        """Print DataFrame to logs"""
        max_rows = dest_config.get('max_rows', 10)
        
        logger.info(f"=== DATA OUTPUT ({len(df)} total rows) ===")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Data types:\n{df.dtypes}")
        logger.info(f"First {max_rows} rows:")
        logger.info(f"\n{df.head(max_rows).to_string()}")
        
        if len(df) > max_rows:
            logger.info(f"... and {len(df) - max_rows} more rows")
        
        logger.info("=== END DATA OUTPUT ===")
        
        return f"Printed {len(df)} rows to logs"