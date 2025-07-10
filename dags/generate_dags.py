# File: dags/generate_dags.py
"""
This file automatically generates DAGs from YAML configurations with external transform support
Place this file in your dags/ directory and it will create all DAGs
"""

import sys
import os

# Add the dags directory to Python path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.dag_factory import DAGFactory
import logging

logger = logging.getLogger(__name__)

# Create DAG factory
dag_factory = DAGFactory()

# Generate all DAGs from configurations
try:
    generated_dags = dag_factory.create_all_dags()
    
    # Make DAGs available to Airflow
    for dag in generated_dags:
        globals()[dag.dag_id] = dag
    
    logger.info(f"Successfully generated {len(generated_dags)} DAGs")
    
    # Log summary of features
    dags_with_external_transforms = [dag for dag in generated_dags if 'external_transforms' in dag.tags]
    standard_dags = [dag for dag in generated_dags if 'external_transforms' not in dag.tags]
    
    logger.info(f"DAGs with external transforms: {len(dags_with_external_transforms)}")
    logger.info(f"Standard DAGs: {len(standard_dags)}")
    
    if dags_with_external_transforms:
        logger.info(f"DAG IDs: {[dag.dag_id for dag in dags_with_external_transforms]}")
    
except Exception as e:
    logger.error(f"Error generating DAGs: {e}")
    logger.error(f"Make sure all configuration files are valid and external transform files exist")
    raise
