# File: dags/utils/config_loader.py
"""
Configuration loader that validates and processes YAML configs
"""

import yaml
import os
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Loads and validates data source configurations using external schema files"""
    
    def __init__(self, config_dir="/opt/airflow/config"):
        self.config_dir = config_dir
        self.schemas_dir = os.path.join(config_dir, "schemas")
        self.validation_config = self._load_validation_config()
        self.schemas = self._load_schema_files()
    
    def _load_validation_config(self) -> Dict[str, Any]:
        """Load validation configuration"""
        validation_config_path = os.path.join(self.config_dir, "validation_config.yaml")
        
        if os.path.exists(validation_config_path):
            try:
                with open(validation_config_path, 'r') as f:
                    config = yaml.safe_load(f)
                logger.info("Loaded validation configuration from file")
                return config
            except Exception as e:
                logger.warning(f"Error loading validation config: {e}")
        
        # Default validation config
        return {
            'validation': {
                'enabled': True,
                'on_failure': 'warn',
                'use_schema_files': True,
                'allow_unknown_fields': True,
                'log_validation_details': True
            }
        }
    
    def _load_schema_files(self) -> Dict[str, Any]:
        """Load schema files from the schemas directory"""
        schemas = {}
        
        if not os.path.exists(self.schemas_dir):
            logger.warning(f"Schemas directory not found: {self.schemas_dir}")
            return schemas
        
        schema_files = {
            'data_source': 'data_source_schema.yaml',
            'transformation': 'transformation_schema.yaml',
            'enrichment': 'enrichment_schema.yaml'
        }
        
        for schema_name, filename in schema_files.items():
            filepath = os.path.join(self.schemas_dir, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r') as f:
                        schemas[schema_name] = yaml.safe_load(f)
                    logger.info(f"Loaded {schema_name} schema from {filename}")
                except Exception as e:
                    logger.warning(f"Error loading schema {filename}: {e}")
            else:
                logger.info(f"Schema file not found: {filename} (optional)")
        
        return schemas
    
    def load_data_source_configs(self) -> List[Dict[str, Any]]:
        """Load all data source configurations from YAML files"""
        configs = []
        data_sources_dir = os.path.join(self.config_dir, "data_sources")
        
        if not os.path.exists(data_sources_dir):
            logger.warning(f"Data sources directory not found: {data_sources_dir}")
            return configs
        
        for filename in os.listdir(data_sources_dir):
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                filepath = os.path.join(data_sources_dir, filename)
                try:
                    config = self._load_config_file(filepath)
                    if self._validate_config(config, filename):
                        configs.append(config)
                except Exception as e:
                    logger.error(f"Error loading config {filename}: {e}")
                    continue
        
        return configs
    
    def load_global_settings(self) -> Dict[str, Any]:
        """Load global settings configuration"""
        global_config_path = os.path.join(self.config_dir, "global_settings.yaml")
        
        if os.path.exists(global_config_path):
            return self._load_config_file(global_config_path)
        
        # Return defaults if no global config
        return {
            'default_settings': {
                'retry_count': 3,
                'timeout': 30,
                'email_on_failure': True,
                'email_on_retry': False
            }
        }
    
    def _load_config_file(self, filepath: str) -> Dict[str, Any]:
        """Load a single YAML configuration file"""
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    
    def _validate_config(self, config: Dict[str, Any], filename: str) -> bool:
        """Validate configuration against schema"""
        # For now, skip complex validation and just check basic required fields
        if 'data_source' not in config:
            logger.error(f"Missing 'data_source' in {filename}")
            return False
        
        if 'name' not in config['data_source']:
            logger.error(f"Missing 'name' in data_source config in {filename}")
            return False
        
        if 'destination' not in config:
            logger.error(f"Missing 'destination' in {filename}")
            return False
        
        logger.info(f"✅ Basic validation passed for {filename}")
        return True
        """Create configuration validator with schema"""
        if not HAS_CERBERUS:
            return None
        schema = {
            'data_source': {
                'type': 'dict',
                'required': True,
                'schema': {
                    'name': {'type': 'string', 'required': True},
                    'type': {'type': 'string', 'required': True, 'allowed': ['rest_api', 'sftp']},
                    'endpoint': {'type': 'string'},
                    'connection_id': {'type': 'string'},
                    'authentication': {
                        'type': 'dict',
                        'schema': {
                            'type': {'type': 'string', 'allowed': ['oauth', 'bearer_token', 'api_key', 'none']},
                            'credentials': {'type': 'string', 'required': False}
                        }
                    }
                }
            },
            'destination': {
                'type': 'dict',
                'required': True,
                'schema': {
                    'primary': {
                        'type': 'dict',
                        'required': True,
                        'schema': {
                            'type': {'type': 'string', 'allowed': ['snowflake_table', 'azure_data_lake', 'azure_blob', 'local_file', 'print_logs']},
                            'table': {'type': 'string'},
                            'container': {'type': 'string'},
                            'path': {'type': 'string'},
                            'filename': {'type': 'string'},
                            'format': {'type': 'string'},
                            'max_rows': {'type': 'integer'}
                        }
                    }
                }
            },
            'schedule': {
                'type': 'dict',
                'schema': {
                    'interval': {'type': 'string'},
                    'start_date': {'type': 'string'},
                    'catchup': {'type': 'boolean'},
                    'tags': {'type': 'list', 'schema': {'type': 'string'}}
                }
            }
        }
        
        return Validator(schema)