# syntax=docker/dockerfile:1

# Global ARGs - available in FROM instructions
ARG AIRFLOW_VERSION=3.1.6
ARG PYTHON_VERSION=3.12
ARG ACR_REGISTRY=rlmsbxdpluksacr.azurecr.io

FROM ${ACR_REGISTRY}/apache/airflow:${AIRFLOW_VERSION} AS builder

# Re-declare ARGs needed in this stage (ARGs don't persist across FROM)
ARG PYTHON_VERSION

USER root

# Copy ZScaler/Corporate CA certificate (needed for pip to work)
COPY zscaler-ca.crt /usr/local/share/ca-certificates/zscaler-ca.crt
RUN update-ca-certificates \
    && cat /usr/local/share/ca-certificates/zscaler-ca.crt >> $(python -c "import certifi; print(certifi.where())")

# Create virtual environment for clean dependency installation
USER airflow

# Copy requirements first (for better layer caching)
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# Copy and build the rlam-airflow-framework package
COPY --chown=airflow:root pyproject.toml /tmp/
COPY --chown=airflow:root rlam_airflow_framework/ /tmp/rlam_airflow_framework/

# Install dependencies with optimisations:
# --mount=type=cache: BuildKit caches pip downloads externally (not in image layer)
#                     This gives FAST rebuilds AND small images
# --user: Install to user site-packages
# --compile: Pre-compile Python files to .pyc (faster startup)
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0 \
    pip install --user --compile -r /tmp/requirements.txt \
    && cd /tmp \
    && pip install --user --compile --no-deps . \
    && find /home/airflow/.local -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true \
    && find /home/airflow/.local -type f -name "*.pyc" -delete 2>/dev/null || true \
    && find /home/airflow/.local -type f -name "*.pyo" -delete 2>/dev/null || true \
    && find /home/airflow/.local -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true \
    && find /home/airflow/.local -type d -name "test" -exec rm -rf {} + 2>/dev/null || true \
    && find /home/airflow/.local -type f -name "*.md" -delete 2>/dev/null || true \
    && find /home/airflow/.local -type f -name "*.rst" -delete 2>/dev/null || true \
    && find /home/airflow/.local -type f -name "*.txt" ! -name "requirements*.txt" -delete 2>/dev/null || true

FROM ${ACR_REGISTRY}/apache/airflow:${AIRFLOW_VERSION} AS final

# Re-declare ARGs needed in this stage
ARG ACR_REGISTRY
ARG PYTHON_VERSION

# Labels for image metadata
LABEL maintainer="DataPipeline Team" \
      version="1.0.0" \
      description="Config-Driven Data Pipeline with Apache Airflow" \
      org.opencontainers.image.source="https://github.com/your-org/ConfigDrivenDataPipeline"

USER root

COPY zscaler-ca.crt /usr/local/share/ca-certificates/zscaler-ca.crt
RUN update-ca-certificates \
    && cat /usr/local/share/ca-certificates/zscaler-ca.crt >> $(python -c "import certifi; print(certifi.where())")

# CREATE DIRECTORIES
RUN mkdir -p /opt/airflow/config/schemas \
    && mkdir -p /opt/airflow/config/data_sources \
    && mkdir -p /tmp/airflow_output \
    && chown -R airflow:root /opt/airflow/config \
    && chown -R airflow:root /tmp/airflow_output

# COPY PRE-BUILT DEPENDENCIES FROM BUILDER
COPY --from=builder --chown=airflow:root /home/airflow/.local /home/airflow/.local

USER airflow

# Ensure user site-packages are in PATH
# PYTHONDONTWRITEBYTECODE=1: Disable bytecode generation at runtime (already pre-compiled)
# PYTHONUNBUFFERED=1: Unbuffered output for better logging
# PYTHONOPTIMIZE=1: Optimize Python for production
ENV PATH="/home/airflow/.local/bin:${PATH}" \
    PYTHONPATH="/home/airflow/.local/lib/python${PYTHON_VERSION}/site-packages" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD airflow db check || exit 1
