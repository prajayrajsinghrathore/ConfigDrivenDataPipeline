# File: dags/utils/generic_operators.py
"""
Generic operators with external transformation support
"""

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.context import Context
from typing import Dict, Any, Optional
import logging

from utils.data_fetchers import fetch_http_data, fetch_sftp_data
from dags.utils.data_transformers import transform_data, enrich_from_snowflake
from utils.data_loaders import load_to_snowflake, load_to_snowflake_stage, load_to_azure_data_lake, load_to_azure_blob
from utils.kafka_publisher import kafka_publisher
from datetime import datetime, timezone

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
        
        topic = "default_topic_ingestion"
        event = self.config.get('event', {})
        if not event or 'topic' not in event or event['topic'] is None:
            topic = 'no_topic_ingestion'
        else:
            topic = event['topic'] + '_ingestion'

        df = None
        try:
            if source_type == 'rest_api':
                df = self._fetch_from_api()
            elif source_type == 'sftp':
                df = self._fetch_from_sftp()
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            if df is None or df.empty:
                logger.warning(f"No data fetched from {source_type}. Returning empty DataFrame.")
                return pd.DataFrame()
            
            # Add ingestion metadata
            df['ingestion_timestamp'] = datetime.now(timezone.utc)
            df['ingestion_source'] = source_type
            df['dag_id'] = context['dag'].dag_id
            df['task_id'] = context['task'].task_id
            
            # Publish success event
            publish_pipeline_event = kafka_publisher.publish_pipeline_event(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type=source_type,
                status='success',
                message=f"Data ingestion from {source_type} completed successfully. Records: {len(df)}",
                execution_date=datetime.now(timezone.utc),
                topic=topic
            )
            if not publish_pipeline_event:
                logger.error("Failed to publish data ingestion event to Kafka")
            
            logger.info(f"Successfully ingested {len(df)} records from {source_type}")
            return df
            
        except Exception as e:
            # Publish failure event
            publish_pipeline_event = kafka_publisher.publish_pipeline_event(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type=source_type,
                status='failure',
                message=f"Data ingestion failed. Error: {str(e)}",
                execution_date=datetime.now(timezone.utc),
                topic=topic
            )
            if not publish_pipeline_event:
                logger.error("Failed to publish data ingestion failure event to Kafka")
            
            logger.error(f"Data ingestion failed: {str(e)}")
            raise

    def _fetch_from_api(self) -> pd.DataFrame:
        """Fetch data from REST API"""
        endpoint = self.data_source_config['endpoint']
        auth_config = self.data_source_config.get('authentication', {})
        
        # Extract API key from authentication config
        api_key = None
        if auth_config.get('type') in ['bearer_token', 'api_key']:
            api_key = auth_config.get('credentials')
        
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
    Generic operator that applies transformations with external file support
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
            self.input_data = context['task_instance'].xcom_pull(task_ids='ingest_data')
        
        if self.input_data is None or (isinstance(self.input_data, list) and len(self.input_data) == 0):
            logger.warning("No input data provided for transformation. Returning empty DataFrame.")
            return []
        
        # Convert to DataFrame if needed
        if isinstance(self.input_data, list):
            df = pd.DataFrame(self.input_data)
        else:
            df = self.input_data.copy()
        
        topic = "default_topic_transformation"
        event = self.config.get('event', {})
        if not event or 'topic' not in event or event['topic'] is None:
            topic = 'no_topic_transformation'
        else:
            topic = event['topic'] + '_transformation'

        try:
            logger.info(f"Starting transformation on {len(df)} records")
            
            # Apply validation rules
            if 'validation_rules' in self.config:
                df = self._apply_validation(df)
                logger.info(f"After validation: {len(df)} records")
            
            # Apply transformations (including external transforms)
            if 'transformation' in self.config:
                df = transform_data(df, self.config['transformation'])
                logger.info(f"After transformation: {len(df)} records")
            
            # Apply enrichment
            if 'enrichment' in self.config:
                df = self._apply_enrichment(df)
                logger.info(f"After enrichment: {len(df)} records")
            
            # Add transformation metadata
            df['transformation_timestamp'] = datetime.now(timezone.utc)
            df['transformation_version'] = self.config.get('transformation', {}).get('version', '1.0')
            df['dag_run_id'] = context['dag_run'].run_id
            
            logger.info(f"Transformation complete. Result: {len(df)} rows")
            
            # Convert to JSON-serializable format for XCom
            result = []
            if len(df) > 0:
                for _, row in df.iterrows():
                    record = {}
                    for col in df.columns:
                        value = row[col]
                        if pd.isna(value):
                            record[col] = None
                        elif isinstance(value, (list, dict)):
                            record[col] = value
                        else:
                            record[col] = str(value)
                    result.append(record)
            
            # Publish success event
            publish_pipeline_event = kafka_publisher.publish_pipeline_event(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='transformation',
                status='success',
                message=f"Transformation completed successfully. Records: {len(result)}",
                execution_date=datetime.now(timezone.utc),
                topic=topic
            )
            if not publish_pipeline_event:
                logger.error("Failed to publish transformation success event to Kafka")

            return result
            
        except Exception as e:
            # Publish failure event
            publish_pipeline_event = kafka_publisher.publish_pipeline_event(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='transformation',
                status='failure',
                message=f"Transformation failed: {str(e)}",
                execution_date=datetime.now(timezone.utc),
                topic=topic
            )
            if not publish_pipeline_event:
                logger.error("Failed to publish transformation failure event to Kafka")
            
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply validation rules from configuration"""
        validation_rules = self.config['validation_rules']
        
        initial_count = len(df)
        
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
                    df[field] = pd.to_datetime(df[field], errors='coerce')
                # Remove rows with invalid dates
                df = df[df[field].notna()]
            
            elif rule_type == 'string':
                if 'max_length' in rule:
                    max_len = rule['max_length']
                    df = df[df[field].str.len() <= max_len]
                
                # Remove null values if required
                if rule.get('required', False):
                    df = df[df[field].notna()]
        
        validation_removed = initial_count - len(df)
        if validation_removed > 0:
            logger.info(f"Validation removed {validation_removed} records")
        
        return df
    
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
                
                logger.info(f"Applying reference lookup from {enrichment['lookup_table']}")
                df = enrich_from_snowflake(df, 'snowflake-default', lookup_config)
        
        return df


class GenericDataLoadOperator(BaseOperator):
    """
    Generic operator that loads data with improved monitoring
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
            
        if df is None or len(df) == 0:
            logger.warning("No data to load")
            return "No data to load"
        
        destination_config = self.config['destination']

        topic = "default_topic"
        event = self.config.get('event', {})
        if not event or 'topic' not in event or event['topic'] is None:
            topic = 'no_topic'
        else:
            topic = event['topic']
        
        results = []
        
        # Add load metadata
        df['load_timestamp'] = datetime.now(timezone.utc)
        df['load_batch_id'] = f"batch_{context['dag_run'].run_id}_{context['task'].task_id}"
        df['record_count'] = len(df)
        
        # Load to primary destination
        if 'primary' in destination_config:
            result = self._load_to_destination(df, destination_config['primary'], topic, context, 'primary')
            results.append(f"Primary: {result}")
        
        # Load to backup destination
        if 'backup' in destination_config:
            result = self._load_to_destination(df, destination_config['backup'], topic, context, 'backup')
            results.append(f"Backup: {result}")
        
        # Load to archive destination
        if 'archive' in destination_config:
            result = self._load_to_destination(df, destination_config['archive'], topic, context, 'archive')
            results.append(f"Archive: {result}")
        
        final_result = " | ".join(results)
        logger.info(f"Data loading completed: {final_result}")
        return final_result
    
    def _load_to_destination(self, df: pd.DataFrame, dest_config: Dict[str, Any], 
                           topic: str, context: Context, dest_type_label: str) -> str:
        """Load data to a specific destination with monitoring"""
        dest_type = dest_config['type']
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id

        try:
            result_message = ""
            load_start_time = datetime.now()
            
            if dest_type == 'snowflake_table':
                table = dest_config['table']
                mode = dest_config.get('mode', 'append')
                
                result_message = load_to_snowflake(
                    df=df,
                    snowflake_conn_id='snowflake-default',
                    table_name=table.split('.')[-1],
                    schema=table.split('.')[-2] if '.' in table else 'PUBLIC',
                    if_exists=mode
                )
        
            elif dest_type == 'snowflake_stage':
                result_message = load_to_snowflake_stage(
                    df=df,
                    snowflake_conn_id='snowflake-default',
                    stage_name='FINNHUB_STAGE',
                    file_name='finnhub_test_file.csv'
                )
        
            elif dest_type == 'azure_data_lake':
                container = dest_config['container']
                path = dest_config['path']
                file_format = dest_config.get('format', 'parquet')
                
                result_message = load_to_azure_data_lake(
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
                
                result_message = load_to_azure_blob(
                    df=df,
                    azure_conn_id='azure_blob_default',
                    container=container,
                    blob_name=path,
                    file_format=file_format
                )
            
            elif dest_type == 'local_file':
                result_message = self._load_to_local_file(df, dest_config)
            
            elif dest_type == 'print_logs':
                result_message = self._load_to_logs(df, dest_config)
            
            else:
                raise ValueError(f"Unsupported destination type: {dest_type}")
            
            # Calculate load metrics
            load_duration = (datetime.now() - load_start_time).total_seconds()
            
            # Publish data to Kafka
            event_publish_status = kafka_publisher.publish_data(
                dag_id=dag_id,
                data=df.to_dict(orient='records'),
                topic=topic,
                status='success'
            )
            if not event_publish_status:
                logger.error("Failed to publish data to Kafka")
                
            # Publish pipeline event
            publish_pipeline_event = kafka_publisher.publish_pipeline_event(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='data_load',
                status='success',
                message=f"Data loaded successfully to {dest_type}. Records: {len(df)}, Duration: {load_duration:.2f}s",
                execution_date=datetime.now(timezone.utc),
                topic=topic + "_load"
            )
            if not publish_pipeline_event:
                logger.error("Failed to publish load success event to Kafka")
        
            logger.info(f"Successfully loaded {len(df)} records to {dest_type_label} destination")
            
            return f"{result_message} (Duration: {load_duration:.2f}s)"
            
        except Exception as e:
            # Publish failure event
            publish_pipeline_event = kafka_publisher.publish_pipeline_event(
                dag_id=context['dag'].dag_id,
                task_id=context['task'].task_id,
                event_type='data_load',
                status='failure',
                message=f"Data load failed for {dest_type}: {str(e)}",
                execution_date=datetime.now(timezone.utc),
                topic=topic + "_load"
            )
            if not publish_pipeline_event:
                logger.error("Failed to publish load failure event to Kafka")
    
            logger.error(f"Failed to load data to {dest_type_label} destination: {dest_type}")            
            raise

    def _load_to_local_file(self, df: pd.DataFrame, dest_config: Dict[str, Any]) -> str:
        """Save DataFrame to local file with metadata"""
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
        
        # Add file metadata to DataFrame
        df_with_metadata = df.copy()
        df_with_metadata['file_export_timestamp'] = datetime.now()
        df_with_metadata['export_format'] = file_format
        df_with_metadata['export_path'] = filepath
        
        # Save file based on format
        if file_format == 'csv':
            df_with_metadata.to_csv(filepath, index=False)
        elif file_format == 'json':
            df_with_metadata.to_json(filepath, orient='records', indent=2)
        elif file_format == 'parquet':
            df_with_metadata.to_parquet(filepath, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Get file size
        file_size = os.path.getsize(filepath)
        
        logger.info(f"Saved {len(df)} rows to {filepath} ({file_size} bytes)")
        return f"Saved to {filepath} ({file_size} bytes)"
    
    def _load_to_logs(self, df: pd.DataFrame, dest_config: Dict[str, Any]) -> str:
        """Print DataFrame to logs with formatting"""
        max_rows = dest_config.get('max_rows', 10)
        
        logger.info(f"=== DATA OUTPUT ({len(df)} total rows) ===")
        logger.info(f"Columns ({len(df.columns)}): {list(df.columns)}")
        logger.info(f"Data types:\n{df.dtypes}")
        
        # Show data quality summary
        quality_summary = {
            'total_records': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB"
        }
        logger.info(f"Data Quality Summary: {quality_summary}")
        
        logger.info(f"First {max_rows} rows:")
        logger.info(f"\n{df.head(max_rows).to_string()}")
        
        if len(df) > max_rows:
            logger.info(f"... and {len(df) - max_rows} more rows")
        
        logger.info("=== END DATA OUTPUT ===")
        
        return f"Printed {len(df)} rows to logs with formatting"