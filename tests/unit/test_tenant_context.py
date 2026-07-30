import pytest

from rlam_airflow_framework.tenant_context import TenantContext, TenantValidationError


def _make_context(tenant_kafka_config=None):
    tenants = {
        "acme": {
            "pool": "acme_pool",
            "slots": 4,
        }
    }
    if tenant_kafka_config is not None:
        tenants["acme"]["kafka"] = tenant_kafka_config
    return TenantContext({"tenants": tenants})


def _make_two_tenant_context():
    return TenantContext(
        {
            "tenants": {
                "acme": {"connections": {"snowflake": "acme_snowflake_conn"}},
                "globex": {"connections": {"snowflake": "globex_snowflake_conn"}},
            }
        }
    )


def test_get_tenant_kafka_bootstrap_servers_defaults_to_none():
    # Today every tenant shares the one global Kafka cluster - no override
    # configured means "use the default".
    ctx = _make_context()
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") is None


def test_get_tenant_kafka_bootstrap_servers_no_kafka_block():
    ctx = _make_context(tenant_kafka_config=None)
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") is None


def test_get_tenant_kafka_bootstrap_servers_returns_override():
    ctx = _make_context(
        tenant_kafka_config={"bootstrap_servers": "tenant-cluster:9092"}
    )
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") == "tenant-cluster:9092"


def test_get_tenant_kafka_bootstrap_servers_coexists_with_topic_prefix():
    ctx = _make_context(
        tenant_kafka_config={
            "topic_prefix": "acme",
            "bootstrap_servers": "tenant-cluster:9092",
        }
    )
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") == "tenant-cluster:9092"
    assert ctx.resolve_kafka_topic("orders", "acme") == "acme.orders"


def test_resolve_connection_id_rejects_cross_tenant_connection():
    # acme referencing globex's registered connection is a spoofing attempt
    # (or config error) and must be rejected at resolution time, not just
    # for destinations - any block with a connection_id goes through this.
    ctx = _make_two_tenant_context()
    with pytest.raises(TenantValidationError):
        ctx.resolve_connection_id(
            {"connection_id": "globex_snowflake_conn"}, "snowflake_table", "acme"
        )


def test_resolve_connection_id_allows_own_tenant_connection():
    ctx = _make_two_tenant_context()
    conn_id = ctx.resolve_connection_id(
        {"connection_id": "acme_snowflake_conn"}, "snowflake_table", "acme"
    )
    assert conn_id == "acme_snowflake_conn"


def test_resolve_connection_id_allows_unregistered_shared_connection():
    # A connection_id not registered to any tenant (e.g. a shared/global
    # connection) is intentionally allowed through.
    ctx = _make_two_tenant_context()
    conn_id = ctx.resolve_connection_id(
        {"connection_id": "shared_http_default"}, "http", "acme"
    )
    assert conn_id == "shared_http_default"
