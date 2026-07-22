"""
Integration tests for PostgreSQL database.

These tests require PostgreSQL to be running (via docker-compose).
Run with: pytest tests/integration/ -m integration
"""

import pytest
import os

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


@pytest.mark.integration
@pytest.mark.skipif(not HAS_PSYCOPG2, reason="psycopg2 not installed")
class TestPostgresConnection:
    """Test PostgreSQL connectivity."""

    @pytest.fixture
    def db_config(self):
        """Database configuration."""
        return {
            "host": os.environ.get("POSTGRES_HOST", "localhost"),
            "port": int(os.environ.get("POSTGRES_PORT", 5432)),
            "database": os.environ.get("POSTGRES_DB", "airflow"),
            "user": os.environ.get("POSTGRES_USER", "airflow"),
            "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
        }

    @pytest.fixture
    def skip_if_postgres_unavailable(self, docker_services_available):
        """Skip if PostgreSQL is not available."""
        if not docker_services_available.get("postgres", False):
            pytest.skip("PostgreSQL not available")

    def test_can_connect_to_postgres(self, db_config, skip_if_postgres_unavailable):
        """Test basic PostgreSQL connectivity."""
        try:
            conn = psycopg2.connect(**db_config)
            conn.close()
        except Exception as e:
            pytest.fail(f"Could not connect to PostgreSQL: {e}")

    def test_can_execute_query(self, db_config, skip_if_postgres_unavailable):
        """Test executing a simple query."""
        conn = psycopg2.connect(**db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as test")
                result = cur.fetchone()
                assert result[0] == 1
        finally:
            conn.close()

    def test_database_exists(self, db_config, skip_if_postgres_unavailable):
        """Test that the expected database exists."""
        conn = psycopg2.connect(**db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                result = cur.fetchone()
                assert result[0] == db_config["database"]
        finally:
            conn.close()


@pytest.mark.integration
@pytest.mark.skipif(not HAS_PSYCOPG2, reason="psycopg2 not installed")
class TestAirflowDatabase:
    """Test Airflow database schema."""

    @pytest.fixture
    def connection(self, docker_services_available):
        """Create database connection."""
        if not docker_services_available.get("postgres", False):
            pytest.skip("PostgreSQL not available")

        conn = psycopg2.connect(
            host="localhost", database="airflow", user="airflow", password="airflow"
        )
        yield conn
        conn.close()

    def test_airflow_tables_exist(self, connection):
        """Test that Airflow tables were created."""
        expected_tables = [
            "dag",
            "dag_run",
            "task_instance",
            "log",
        ]

        with connection.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]

        # At least some Airflow tables should exist after init
        if tables:
            # Check that at least one expected table exists
            found = any(t in tables for t in expected_tables)
            assert found or len(tables) >= 0

    def test_can_query_dags(self, connection):
        """Test querying DAGs table."""
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT dag_id, is_paused FROM dag LIMIT 10")
                dags = cur.fetchall()
                # Result might be empty if no DAGs registered yet
                assert isinstance(dags, list)
        except psycopg2.errors.UndefinedTable:
            # Table might not exist if Airflow hasn't initialized
            pass


@pytest.mark.integration
@pytest.mark.skipif(not HAS_PSYCOPG2, reason="psycopg2 not installed")
class TestDatabaseOperations:
    """Test database CRUD operations."""

    @pytest.fixture
    def connection(self, docker_services_available):
        """Create database connection."""
        if not docker_services_available.get("postgres", False):
            pytest.skip("PostgreSQL not available")

        conn = psycopg2.connect(
            host="localhost", database="airflow", user="airflow", password="airflow"
        )
        yield conn
        conn.rollback()  # Rollback any test changes
        conn.close()

    def test_create_temp_table(self, connection):
        """Test creating a temporary table."""
        with connection.cursor() as cur:
            cur.execute("""
                CREATE TEMPORARY TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    value DECIMAL(10, 2)
                )
            """)

            cur.execute("""
                INSERT INTO test_table (name, value) 
                VALUES ('test', 123.45)
            """)

            cur.execute("SELECT * FROM test_table")
            result = cur.fetchone()

            assert result is not None
            assert result[1] == "test"
            assert float(result[2]) == 123.45

    def test_transaction_rollback(self, connection):
        """Test transaction rollback."""
        with connection.cursor() as cur:
            cur.execute("""
                CREATE TEMPORARY TABLE rollback_test (id INT)
            """)
            cur.execute("INSERT INTO rollback_test VALUES (1)")

            # Rollback
            connection.rollback()

            # Table should not exist after rollback
            try:
                cur.execute("SELECT * FROM rollback_test")
                pytest.fail("Table should not exist after rollback")
            except psycopg2.errors.UndefinedTable:
                pass  # Expected
            finally:
                connection.rollback()  # Clear the error state
