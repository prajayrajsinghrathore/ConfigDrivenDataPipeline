"""
Config-Driven Data Pipeline Test Suite.

Test structure:
- unit/        - Fast unit tests, no external dependencies
- contract/    - Schema/contract validation tests
- dag/         - Airflow DAG integrity tests
- integration/ - Integration tests (require Docker services)
- e2e/         - End-to-end pipeline tests (require full stack)

Run commands:
    # Unit tests only (fast)
    pytest tests/unit/ -v

    # All tests without integration/e2e
    pytest tests/ -v --ignore=tests/integration --ignore=tests/e2e

    # Integration tests (requires: docker-compose up -d postgres kafka redis)
    pytest tests/integration/ -v -m integration

    # Full E2E tests (requires: docker-compose up -d)
    pytest tests/e2e/ -v -m e2e --timeout=300

    # All tests with coverage
    pytest --cov=dags --cov-report=html --cov-report=term

    # Run specific test markers
    pytest -m "unit and not slow"
    pytest -m "integration"
    pytest -m "e2e"
"""
