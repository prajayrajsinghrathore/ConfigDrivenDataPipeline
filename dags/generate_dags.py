# File: dags/generate_dags.py
"""
This file automatically generates DAGs from YAML configurations.
Place this file in your dags/ directory and it will create all DAGs.

Hot-reload is handled by Airflow's built-in mechanisms:
- Production (AKS): CI/CD pushes configs to volume, Airflow refreshes via dag_dir_list_interval
- Local (Docker): Volume mount to /config, Airflow picks up changes on next parse cycle

Airflow settings to tune refresh speed (airflow.cfg):
- dag_dir_list_interval: How often to scan for new DAG files (default: 300s)
- min_file_process_interval: Min time between re-parsing a DAG file (default: 30s)
"""

import logging
from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

logger = logging.getLogger(__name__)

# Create DAG factory
dag_factory = DAGFactoryV2()

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
