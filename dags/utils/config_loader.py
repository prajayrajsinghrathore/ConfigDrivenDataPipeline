# File: dags/utils/config_loader.py
"""
Configuration loader with external transformation support
"""

import yaml
import os
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """configuration loader that supports external transformations"""
    
    def __init__(self, config_dir="/opt/airflow/config"):
        self.config_dir = config_dir
        self.schemas_dir = os.path.join(config_dir, "schemas")
        self.transforms_dir = os.path.join(config_dir, "transforms")  # Already correct
        self.validation_config = self._load_validation_config()
        self.schemas = self._load_schema_files()
        
        # Ensure transforms directory exists
        os.makedirs(self.transforms_dir, exist_ok=True)
    
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
        
        # Default validation config with external transform support
        return {
            'validation': {
                'enabled': True,
                'on_failure': 'warn',
                'use_schema_files': True,
                'allow_unknown_fields': True,
                'log_validation_details': True,
                'validate_external_transforms': True,
                'external_transform_timeout': 300  # 5 minutes
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
            'enrichment': 'enrichment_schema.yaml',
            'external_transforms': 'external_transforms_schema.yaml'
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
                        # Process external transformations
                        config = self._process_external_transformations(config, filename)
                        configs.append(config)
                except Exception as e:
                    logger.error(f"Error loading config {filename}: {e}")
                    continue
        
        return configs
    
    def _process_external_transformations(self, config: Dict[str, Any], filename: str) -> Dict[str, Any]:
        """Process and validate external transformations in the config"""
        if 'transformation' not in config:
            return config
        
        transformation = config['transformation']
        
        if 'external_transforms' in transformation:
            for i, ext_transform in enumerate(transformation['external_transforms']):
                transform_type = ext_transform.get('type')
                
                if transform_type == 'python_file':
                    # Validate Python file exists and is accessible
                    file_path = ext_transform.get('file_path')
                    if file_path and not self._validate_python_file(file_path):
                        logger.warning(f"Python file {file_path} not found or not accessible in {filename}")
                        ext_transform['validation_error'] = f"File not found: {file_path}"
                
                elif transform_type == 'beam_yaml':
                    # Validate Beam YAML syntax
                    yaml_config = ext_transform.get('yaml_config')
                    if yaml_config and not self._validate_beam_yaml(yaml_config):
                        logger.warning(f"Invalid Beam YAML configuration in {filename}")
                        ext_transform['validation_error'] = "Invalid Beam YAML syntax"
                
                elif transform_type == 'custom_function':
                    # Validate custom function syntax
                    function_code = ext_transform.get('function_code')
                    if function_code and not self._validate_custom_function(function_code):
                        logger.warning(f"Invalid custom function code in {filename}")
                        ext_transform['validation_error'] = "Invalid function syntax"
                
                # Add metadata
                ext_transform['config_source'] = filename
                ext_transform['validated_at'] = logger.info(f"Processed external transformation {i+1} in {filename}")
        
        return config
    
    def _validate_python_file(self, file_path: str) -> bool:
        """Validate that a Python file exists and is readable"""
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                return False
            
            # Check if file is readable
            with open(file_path, 'r') as f:
                f.read(1)  # Try to read first character
            
            # Basic syntax check (compile without executing)
            with open(file_path, 'r') as f:
                code = f.read()
                compile(code, file_path, 'exec')
            
            return True
            
        except Exception as e:
            logger.error(f"Python file validation failed for {file_path}: {e}")
            return False
    
    def _validate_beam_yaml(self, yaml_config: str) -> bool:
        """Validate Beam YAML configuration syntax"""
        try:
            # Parse YAML
            config = yaml.safe_load(yaml_config)
            
            # Basic structure validation
            if not isinstance(config, dict):
                return False
            
            # Check for required Beam YAML structure
            if 'pipeline' in config:
                pipeline = config['pipeline']
                if 'transforms' in pipeline:
                    transforms = pipeline['transforms']
                    if not isinstance(transforms, list):
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Beam YAML validation failed: {e}")
            return False
    
    def _validate_custom_function(self, function_code: str) -> bool:
        """Validate custom function code syntax"""
        try:
            # Basic syntax check
            compile(function_code, '<string>', 'exec')
            
            # Check if it contains a function definition
            if 'def ' not in function_code:
                logger.warning("Custom function code does not contain a function definition")
                return False
            
            return True
            
        except SyntaxError as e:
            logger.error(f"Custom function syntax error: {e}")
            return False
        except Exception as e:
            logger.error(f"Custom function validation failed: {e}")
            return False
    
    def load_global_settings(self) -> Dict[str, Any]:
        """Load global settings configuration with external transform settings"""
        global_config_path = os.path.join(self.config_dir, "global_settings.yaml")
        
        if os.path.exists(global_config_path):
            config = self._load_config_file(global_config_path)
        else:
            config = {}
        
        # Add default external transform settings
        if 'external_transforms' not in config:
            config['external_transforms'] = {
                'enabled': True,
                'timeout_seconds': 300,
                'max_memory_mb': 1024,
                'allowed_imports': [
                    'pandas', 'numpy', 'datetime', 'typing', 'logging',
                    'json', 'math', 'statistics', 'collections'
                ],
                'security_mode': 'restricted',
                'cache_results': True,
                'cache_ttl_minutes': 60
            }
        
        # Ensure default settings exist
        if 'default_settings' not in config:
            config['default_settings'] = {
                'retry_count': 3,
                'timeout': 30,
                'email_on_failure': True,
                'email_on_retry': False
            }
        
        return config
    
    def _load_config_file(self, filepath: str) -> Dict[str, Any]:
        """Load a single YAML configuration file"""
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    
    def _validate_config(self, config: Dict[str, Any], filename: str) -> bool:
        """Validate configuration against schema"""
        # Basic validation (existing logic)
        if 'data_source' not in config:
            logger.error(f"Missing 'data_source' in {filename}")
            return False
        
        if 'name' not in config['data_source']:
            logger.error(f"Missing 'name' in data_source config in {filename}")
            return False
        
        if 'destination' not in config:
            logger.error(f"Missing 'destination' in {filename}")
            return False
        
        # Extended validation for external transformations
        if 'transformation' in config and 'external_transforms' in config['transformation']:
            if not self._validate_external_transforms_config(config['transformation']['external_transforms'], filename):
                logger.warning(f"External transforms validation failed in {filename}")
                # Don't fail completely, just log warning
        
        logger.info(f"✅ Validation passed for {filename}")
        return True
    
    def _validate_external_transforms_config(self, external_transforms: List[Dict], filename: str) -> bool:
        """Validate external transforms configuration"""
        try:
            for i, transform in enumerate(external_transforms):
                if 'type' not in transform:
                    logger.error(f"Missing 'type' in external transform {i+1} in {filename}")
                    return False
                
                transform_type = transform['type']
                
                if transform_type == 'python_file':
                    required_fields = ['file_path', 'function_name']
                    for field in required_fields:
                        if field not in transform:
                            logger.error(f"Missing '{field}' in python_file transform {i+1} in {filename}")
                            return False
                
                elif transform_type == 'beam_yaml':
                    if 'yaml_config' not in transform:
                        logger.error(f"Missing 'yaml_config' in beam_yaml transform {i+1} in {filename}")
                        return False
                
                elif transform_type == 'custom_function':
                    if 'function_code' not in transform:
                        logger.error(f"Missing 'function_code' in custom_function transform {i+1} in {filename}")
                        return False
                
                else:
                    logger.warning(f"Unknown external transform type '{transform_type}' in {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating external transforms in {filename}: {e}")
            return False
    
    def save_config_template(self, template_name: str, template_data: Dict[str, Any]) -> str:
        """Save a configuration template for reuse"""
        template_path = os.path.join(self.config_dir, "templates", f"{template_name}.yaml")
        os.makedirs(os.path.dirname(template_path), exist_ok=True)
        
        with open(template_path, 'w') as f:
            yaml.dump(template_data, f, default_flow_style=False, indent=2)
        
        logger.info(f"Saved configuration template: {template_path}")
        return template_path
    
    def load_config_template(self, template_name: str) -> Dict[str, Any]:
        """Load a configuration template"""
        template_path = os.path.join(self.config_dir, "templates", f"{template_name}.yaml")
        
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template not found: {template_path}")
        
        return self._load_config_file(template_path)
    
    def validate_transform_file_permissions(self, file_path: str) -> bool:
        """Validate that transform files have appropriate permissions"""
        try:
            # Check file exists and is readable
            if not os.path.exists(file_path):
                return False
            
            # Check file permissions (readable but not writable by others)
            file_stat = os.stat(file_path)
            file_permissions = oct(file_stat.st_mode)[-3:]
            
            # Should be readable by owner and group, but not world-writable
            if file_permissions[-1] in ['2', '3', '6', '7']:  # World-writable
                logger.warning(f"Transform file {file_path} is world-writable (potential security risk)")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking file permissions for {file_path}: {e}")
            return False