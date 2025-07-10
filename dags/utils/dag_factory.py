# File: dags/utils/dag_factory.py
"""
DAG factory that creates Airflow DAGs from YAML configurations
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging
from dags.utils.config_loader import ConfigLoader  # Updated import
from dags.utils.generic_operators import (  # Updated import
    GenericDataIngestionOperator,
    GenericDataTransformationOperator,
    GenericDataLoadOperator
)

logger = logging.getLogger(__name__)

class DAGFactory:
    """Factory class that creates DAGs from configurations"""
    
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.global_settings = self.config_loader.load_global_settings()
    
    def create_dag_from_config(self, config: Dict[str, Any]) -> DAG:
        """Create a single DAG from configuration"""
        
        data_source = config['data_source']
        schedule_config = config.get('schedule', {})
        
        # Parse schedule configuration
        dag_id = f"data_integration_{data_source['name']}"
        schedule_interval = schedule_config.get('interval', '@daily')
        start_date = datetime.strptime(schedule_config.get('start_date', '2024-01-01'), '%Y-%m-%d')
        catchup = schedule_config.get('catchup', False)
        tags = schedule_config.get('tags', [])
        
        pipeline_tags = tags + ['pipeline']
        if 'transformation' in config and 'external_transforms' in config['transformation']:
            pipeline_tags.append('external_transforms')
        
        # Create default arguments
        default_args = self._create_default_args()
        
        # Create DAG with metadata
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=f"Data integration pipeline for {data_source['name']}",
            schedule=schedule_interval,
            start_date=start_date,
            catchup=catchup,
            tags=pipeline_tags,
            max_active_runs=1,
            params={
                'data_source_name': data_source['name'],
                'has_external_transforms': 'transformation' in config and 'external_transforms' in config.get('transformation', {}),
                'pipeline_version': config.get('metadata', {}).get('version', '1.0.0')
            }
        )
        
        # Create tasks
        self._create_tasks(dag, config)
        
        return dag
    
    def create_all_dags(self) -> List[DAG]:
        """Create all DAGs from configuration files"""
        configs = self.config_loader.load_data_source_configs()
        dags = []
        
        logger.info(f"Loading configurations from config loader...")
        
        for config in configs:
            try:
                dag = self.create_dag_from_config(config)
                dags.append(dag)
                
                # Log external transforms info
                external_transforms = config.get('transformation', {}).get('external_transforms', [])
                if external_transforms:
                    logger.info(f"Created DAG: {dag.dag_id} with {len(external_transforms)} external transforms")
                else:
                    logger.info(f"Created standard DAG: {dag.dag_id}")
                    
            except Exception as e:
                data_source_name = config.get('data_source', {}).get('name', 'unknown')
                logger.error(f"Error creating DAG for {data_source_name}: {e}")
                # Log more details for debugging
                logger.error(f"Config keys: {list(config.keys())}")
                if 'transformation' in config:
                    transformation_keys = list(config['transformation'].keys())
                    logger.error(f"Transformation keys: {transformation_keys}")
        
        return dags
    
    def _create_default_args(self) -> Dict[str, Any]:
        """Create default arguments for DAGs"""
        defaults = self.global_settings.get('default_settings', {})
        external_transform_settings = self.global_settings.get('external_transforms', {})
        
        return {
            'owner': 'data-team',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'email_on_failure': defaults.get('email_on_failure', True),
            'email_on_retry': defaults.get('email_on_retry', False),
            'retries': defaults.get('retry_count', 3),
            'retry_delay': timedelta(minutes=5),
            # Add external transform settings
            'external_transform_timeout': external_transform_settings.get('timeout_seconds', 300),
            'external_transform_memory_limit': external_transform_settings.get('max_memory_mb', 1024),
        }
    
    def _create_tasks(self, dag: DAG, config: Dict[str, Any]):
        """Create tasks for a DAG based on configuration"""
        
        # Task 1: Data Ingestion
        ingest_task = GenericDataIngestionOperator(  
            task_id='ingest_data',
            config=config,
            dag=dag
        )
        
        # Task 2: Data Transformation (if needed)
        transform_task = None
        if ('transformation' in config or 
            'validation_rules' in config or 
            'enrichment' in config):
            
            transform_task = GenericDataTransformationOperator(
                task_id='transform_data',
                config=config,
                dag=dag
            )
        
        # Task 3: Data Loading
        load_task = GenericDataLoadOperator(
            task_id='load_data',
            config=config,
            dag=dag
        )
        
        # Set task dependencies
        if transform_task:
            ingest_task >> transform_task >> load_task
        else:
            ingest_task >> load_task
        
        # Add external transform validation task if external transforms are present
        # external_transforms = config.get('transformation', {}).get('external_transforms', [])
        # if external_transforms:
        #     validation_task = self._create_external_transform_validation_task(dag, config)
        #     # Run validation before ingestion
        #     validation_task >> ingest_task
    
    # File: dags/utils/dag_factory.py - Fix for the PythonOperator issue

    def _create_external_transform_validation_task(self, dag: DAG, config: Dict[str, Any]):
        """Create a validation task for external transforms"""
        
        def validate_external_transforms(**context):
            """Validate external transform configurations before pipeline execution"""
            from dags.utils.config_loader import ConfigLoader
            
            loader = ConfigLoader()
            external_transforms = config.get('transformation', {}).get('external_transforms', [])
            
            validation_results = []
            for i, transform in enumerate(external_transforms):
                transform_type = transform.get('type')
                
                if transform_type == 'python_file':
                    file_path = transform.get('file_path')
                    function_name = transform.get('function_name')
                    
                    # Validate file exists and function is callable
                    is_valid = loader._validate_python_file(file_path)
                    validation_results.append({
                        'transform_index': i,
                        'type': transform_type,
                        'file_path': file_path,
                        'function_name': function_name,
                        'is_valid': is_valid
                    })
                    
                elif transform_type == 'beam_yaml':
                    yaml_config = transform.get('yaml_config')
                    is_valid = loader._validate_beam_yaml(yaml_config)
                    validation_results.append({
                        'transform_index': i,
                        'type': transform_type,
                        'is_valid': is_valid
                    })
                    
                elif transform_type == 'custom_function':
                    function_code = transform.get('function_code')
                    is_valid = loader._validate_custom_function(function_code)
                    validation_results.append({
                        'transform_index': i,
                        'type': transform_type,
                        'is_valid': is_valid
                    })
            
            # Log validation results
            for result in validation_results:
                if result['is_valid']:
                    logger.info(f"External transform {result['transform_index']} ({result['type']}) validation passed")
                else:
                    logger.error(f"External transform {result['transform_index']} ({result['type']}) validation failed")
            
            # Fail if any validation failed
            failed_validations = [r for r in validation_results if not r['is_valid']]
            if failed_validations:
                raise ValueError(f"External transform validation failed for {len(failed_validations)} transforms")
            
            return validation_results
        
        return PythonOperator(
            task_id='validate_external_transforms',
            python_callable=validate_external_transforms,
            dag=dag
        )