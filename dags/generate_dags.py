# File: dags/generate_dags.py
"""
This file automatically generates DAGs from YAML configurations
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
    
except Exception as e:
    logger.error(f"Error generating DAGs: {e}")
    raise