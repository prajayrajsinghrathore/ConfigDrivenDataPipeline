# File: dags/utils/data_transformers.py
"""
Data transformation utilities with external file support
"""

import pandas as pd
import importlib.util
import os
import yaml
from typing import Dict, Any, Callable
import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

class ExternalTransformLoader:
    """Handles loading and execution of external transformations"""
    
    def __init__(self):
        self.loaded_modules = {}
        
    def load_python_file(self, file_path: str, function_name: str) -> Callable:
        """Load a function from an external Python file"""
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Transform file not found: {file_path}")
            
            # Create module spec and load
            module_name = os.path.basename(file_path).replace('.py', '')
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            
            if spec is None:
                raise ImportError(f"Could not load spec from {file_path}")
                
            module = importlib.util.module_from_spec(spec)
            
            # Cache the module
            if file_path not in self.loaded_modules:
                spec.loader.exec_module(module)
                self.loaded_modules[file_path] = module
            else:
                module = self.loaded_modules[file_path]
            
            # Get the function
            if not hasattr(module, function_name):
                raise AttributeError(f"Function '{function_name}' not found in {file_path}")
                
            return getattr(module, function_name)
            
        except Exception as e:
            logger.error(f"Error loading Python function {function_name} from {file_path}: {e}")
            raise
    
    def execute_beam_yaml(self, yaml_config: str, df: pd.DataFrame) -> pd.DataFrame:
        """Execute a Beam YAML transformation"""
        try:
            # Parse YAML config
            config = yaml.safe_load(yaml_config)
            
            # For now, simulate Beam YAML execution by extracting Python code
            # In a real implementation, you'd use Apache Beam's YAML capabilities
            transforms = config.get('pipeline', {}).get('transforms', [])
            
            result_df = df.copy()
            
            for transform in transforms:
                if transform.get('type') == 'MapToFields':
                    fields_config = transform.get('config', {}).get('fields', {})
                    
                    for field_name, field_logic in fields_config.items():
                        if isinstance(field_logic, str) and 'def ' in field_logic:
                            try:
                                # Create a safe execution environment
                                local_vars = {'pd': pd, 'element': None}
                                
                                # Execute the function definition
                                exec(field_logic, globals(), local_vars)

                                # Find the function that was defined
                                func_name = None
                                for key, value in local_vars.items():
                                    if callable(value) and key not in ['pd']:
                                        func_name = key
                                        break

                                if func_name:
                                    func = local_vars[func_name]
                                    
                                    # Apply the function to each row
                                    result_df[field_name] = result_df.apply(
                                        lambda row: func(row.to_dict()), axis=1
                                    )
                                else:
                                    logger.warning(f"No function found in field logic for {field_name}")
                                    result_df[field_name] = "BEAM_YAML_ERROR"
                                    
                            except Exception as e:
                                logger.error(f"Error executing Beam YAML field {field_name}: {e}")
                                result_df[field_name] = f"ERROR: {str(e)}"
                        else:
                            # Handle simple string values
                            result_df[field_name] = str(field_logic)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error executing Beam YAML transformation: {e}")
            # Return original dataframe with error column
            df['beam_yaml_error'] = str(e)
            return df
    
    def execute_custom_function(self, function_code: str, df: pd.DataFrame, 
                           parameters: Dict[str, Any] = None, **kwargs) -> pd.DataFrame:
        """Execute custom function code"""
        try:
            # Merge parameters and kwargs
            all_params = {}
            if parameters:
                all_params.update(parameters)
            all_params.update(kwargs)
            
            # Create a safe execution environment
            local_vars = {
                'pd': pd,
                'df': df.copy(),
                'parameters': all_params,
                'np': __import__('numpy'),  # Add numpy support
                'datetime': __import__('datetime'),  # Add datetime support
            }

            # Execute the function definition
            exec(function_code, globals(), local_vars)
            
            # Find the function that was defined
            func_name = None
            for key, value in local_vars.items():
                if callable(value) and key not in ['pd', 'np', 'datetime']:
                    func_name = key
                    break
            
            if func_name is None:
                raise ValueError("No function found in custom function code")
            
            # Execute the function
            func = local_vars[func_name]
            if all_params:
                result = func(df, **all_params)
            else:
                result = func(df)
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing custom function: {e}")
            raise


def transform_data(df: pd.DataFrame, transformations: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply transformations including external files
    
    Args:
        df: Input DataFrame
        transformations: transformation configuration
    
    Returns:
        pandas.DataFrame: Transformed data
    """
    result_df = df.copy()
    loader = ExternalTransformLoader()
    
    try:
        # Apply standard transformations first (existing logic)
        if 'column_types' in transformations:
            for col, dtype in transformations['column_types'].items():
                if col in result_df.columns:
                    if dtype == 'datetime':
                        result_df[col] = pd.to_datetime(result_df[col])
                    else:
                        result_df[col] = result_df[col].astype(dtype)
        
        if 'new_columns' in transformations:
            for col_name, formula in transformations['new_columns'].items():
                try:
                    if '.str.upper()' in formula:
                        source_field = formula.split('.')[0]
                        if source_field in result_df.columns:
                            result_df[col_name] = result_df[source_field].str.upper()
                    elif 'pd.Timestamp.now()' in formula:
                        result_df[col_name] = pd.Timestamp.now()
                    elif ' * ' in formula:
                        parts = formula.split(' * ')
                        if len(parts) == 2 and all(p.strip() in result_df.columns for p in parts):
                            result_df[col_name] = result_df[parts[0].strip()] * result_df[parts[1].strip()]
                    else:
                        # Try to evaluate as pandas expression
                        result_df[col_name] = result_df.eval(formula)
                except Exception as e:
                    logger.warning(f"Error creating column {col_name}: {e}")
                    result_df[col_name] = str(formula)
        
        if 'filters' in transformations:
            for col, condition in transformations['filters'].items():
                if col in result_df.columns:
                    try:
                        result_df = result_df.query(f"{col} {condition}")
                    except Exception as e:
                        logger.warning(f"Error applying filter {col} {condition}: {e}")
        
        # Apply external transformations
        if 'external_transforms' in transformations:
            for ext_transform in transformations['external_transforms']:
                transform_type = ext_transform.get('type')
                
                if transform_type == 'python_file':
                    file_path = ext_transform['file_path']
                    function_name = ext_transform['function_name']
                    parameters = ext_transform.get('parameters', {})
                    
                    logger.info(f"Applying Python file transformation: {file_path}.{function_name}")
                    
                    # Load and execute the function
                    func = loader.load_python_file(file_path, function_name)
                    if parameters:
                        result_df = func(result_df, **parameters)
                    else:
                        result_df = func(result_df)
                
                elif transform_type == 'beam_yaml':
                    yaml_config = ext_transform['yaml_config']
                    
                    logger.info("Applying Beam YAML transformation")
                    result_df = loader.execute_beam_yaml(yaml_config, result_df)
                
                elif transform_type == 'custom_function':
                    function_code = ext_transform['function_code']
                    parameters = ext_transform.get('parameters', {})
                    
                    logger.info("Applying custom function transformation")
                    result_df = loader.execute_custom_function(function_code, result_df, parameters)
                
                else:
                    logger.warning(f"Unknown external transform type: {transform_type}")
        
        logger.info(f"Transformation complete. Result: {len(result_df)} rows")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        raise


def enrich_from_snowflake(df: pd.DataFrame, snowflake_conn_id: str, 
                         lookup_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Enrich DataFrame with data from Snowflake (existing function)
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    lookup_column = list(lookup_config['join_on'].keys())[0]
    unique_values = df[lookup_column].unique().tolist()
    
    placeholders = ','.join([f"'{val}'" for val in unique_values])
    snowflake_column = lookup_config['join_on'][lookup_column]
    
    select_cols = ', '.join([snowflake_column] + lookup_config['select_columns'])
    
    query = f"""
    SELECT {select_cols}
    FROM {lookup_config['table']}
    WHERE {snowflake_column} IN ({placeholders})
    """
    
    lookup_df = hook.get_pandas_df(query)
    
    result_df = df.merge(
        lookup_df,
        left_on=lookup_column,
        right_on=snowflake_column,
        how='left'
    )
    
    return result_df