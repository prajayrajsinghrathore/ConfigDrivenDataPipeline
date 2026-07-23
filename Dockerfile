# syntax=docker/dockerfile:1

# Global ARGs - available in FROM instructions
ARG AIRFLOW_BASE_IMAGE=dhi.io/airflow:3-debian-dev
ARG PYTHON_VERSION=3.13
ARG USE_ZSCALER_CERT=false

FROM ${AIRFLOW_BASE_IMAGE} AS builder

# Re-declare ARGs needed in this stage (ARGs don't persist across FROM)
ARG PYTHON_VERSION
ARG USE_ZSCALER_CERT

USER root

# Copy ZScaler/Corporate CA certificate (needed for pip to work)
COPY zscaler-ca.crt /usr/local/share/ca-certificates/zscaler-ca.crt
RUN if [ "$USE_ZSCALER_CERT" = "true" ] ; then \
        update-ca-certificates \
        && cat /usr/local/share/ca-certificates/zscaler-ca.crt >> $(python -c "import certifi; print(certifi.where())") ; \
    else \
        echo "Skipping ZScaler cert injection" ; \
    fi

# Copy requirements and package source
COPY requirements.txt constraints.txt /tmp/
COPY pyproject.toml /tmp/
COPY rlam_airflow_framework/ /tmp/rlam_airflow_framework/

# Install dependencies globally into the image's own env as root
RUN --mount=type=cache,target=/root/.cache/pip \
    grep -v "^apache-airflow==" /tmp/requirements.txt | grep -v "^apache-airflow-core==" | grep -v "^apache-airflow-task-sdk==" > /tmp/reqs_filtered.txt \
    && pip install --compile -r /tmp/reqs_filtered.txt -c /tmp/constraints.txt \
    && pip uninstall -y apache-airflow apache-airflow-core apache-airflow-task-sdk || true \
    && cd /tmp \
    && pip install --compile --no-deps . \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type f -name "*.pyc" -delete 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type f -name "*.pyo" -delete 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type d -name "test" -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type f -name "*.md" -delete 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type f -name "*.rst" -delete 2>/dev/null || true \
    && find /usr/lib/python${PYTHON_VERSION}/site-packages -type f -name "*.txt" ! -name "requirements*.txt" ! -name "entry_points.txt" ! -name "top_level.txt" -delete 2>/dev/null || true

FROM ${AIRFLOW_BASE_IMAGE} AS final

# Re-declare ARGs needed in this stage
ARG PYTHON_VERSION
ARG USE_ZSCALER_CERT

# Labels for image metadata
LABEL maintainer="DataPipeline Team" \
      version="1.0.0" \
      description="Config-Driven Data Pipeline with Apache Airflow" \
      org.opencontainers.image.source="https://github.com/your-org/ConfigDrivenDataPipeline"

USER root

COPY zscaler-ca.crt /usr/local/share/ca-certificates/zscaler-ca.crt
RUN if [ "$USE_ZSCALER_CERT" = "true" ] ; then \
        update-ca-certificates \
        && cat /usr/local/share/ca-certificates/zscaler-ca.crt >> $(python -c "import certifi; print(certifi.where())") ; \
    else \
        echo "Skipping ZScaler cert injection" ; \
    fi

# CREATE DIRECTORIES
RUN mkdir -p /opt/airflow/config/schemas \
    && mkdir -p /opt/airflow/config/data_sources \
    && mkdir -p /tmp/airflow_output \
    && chown -R airflow:root /opt/airflow/config \
    && chown -R airflow:root /tmp/airflow_output

# COPY PRE-BUILT DEPENDENCIES FROM BUILDER
COPY --from=builder /usr/lib/python${PYTHON_VERSION}/site-packages /usr/lib/python${PYTHON_VERSION}/site-packages

USER airflow

# PYTHONDONTWRITEBYTECODE=1: Disable bytecode generation at runtime (already pre-compiled)
# PYTHONUNBUFFERED=1: Unbuffered output for better logging
# PYTHONOPTIMIZE=1: Optimize Python for production
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD airflow db check || exit 1

ENTRYPOINT ["airflow"]
