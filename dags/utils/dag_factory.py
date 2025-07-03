# File: dags/utils/dag_factory.py
"""
DAG factory that creates Airflow DAGs from YAML configurations
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

from utils.config_loader import ConfigLoader
from utils.generic_operators import (
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
        
        # Create default arguments
        default_args = self._create_default_args()
        
        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=f"Data integration pipeline for {data_source['name']}",
            schedule=schedule_interval,
            start_date=start_date,
            catchup=catchup,
            tags=tags,
            max_active_runs=1
        )
        
        # Create tasks
        self._create_tasks(dag, config)
        
        return dag
    
    def create_all_dags(self) -> List[DAG]:
        """Create all DAGs from configuration files"""
        configs = self.config_loader.load_data_source_configs()
        dags = []
        
        for config in configs:
            try:
                dag = self.create_dag_from_config(config)
                dags.append(dag)
                logger.info(f"Created DAG: {dag.dag_id}")
            except Exception as e:
                logger.error(f"Error creating DAG for {config.get('data_source', {}).get('name', 'unknown')}: {e}")
        
        return dags
    
    def _create_default_args(self) -> Dict[str, Any]:
        """Create default arguments for DAGs"""
        defaults = self.global_settings.get('default_settings', {})
        
        return {
            'owner': 'data-team',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'email_on_failure': defaults.get('email_on_failure', True),
            'email_on_retry': defaults.get('email_on_retry', False),
            'retries': defaults.get('retry_count', 3),
            'retry_delay': timedelta(minutes=5),
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
        if 'transformation' in config or 'validation_rules' in config or 'enrichment' in config:
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