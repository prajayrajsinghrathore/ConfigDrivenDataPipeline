# RLAM Airflow Helm Chart

## Overview

Helm chart for deploying the RLAM config-driven Airflow pipeline to Azure Kubernetes Service (AKS).

## Structure

```
helm/
├── Chart.yaml                    # Chart metadata
├── README.md                     # This file
├── values/
│   ├── production.yaml          # Production (AKS) values
│   └── local.yaml               # Local development values (optional)
└── templates/                   # Helm templates (future)
```

## Usage

### Deploy to Production (AKS)

```bash
helm upgrade --install rlam-airflow apache-airflow/airflow \
  --version 1.15.0 \
  --namespace airflow \
  --values helm/values/production.yaml
```

### Using Official Airflow Chart

This project uses the official Apache Airflow Helm chart with custom values.
We don't maintain custom templates - all customization is done via values files.

**Chart Repository:**
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

## Configuration

See [values/production.yaml](values/production.yaml) for production AKS configuration including:
- DAG Bundles (GitDagBundle for configs)
- Custom Docker image with rlam-airflow-framework package
- Azure integrations (KeyVault, ACR)
- ISTIO service mesh configuration
- Multi-tenancy settings

## Rollback

See [../ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md) for rollback procedures.
