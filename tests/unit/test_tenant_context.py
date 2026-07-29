from rlam_airflow_framework.tenant_context import TenantContext


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


def test_get_tenant_kafka_bootstrap_servers_defaults_to_none():
    # Today every tenant shares the one global Kafka cluster - no override
    # configured means "use the default".
    ctx = _make_context()
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") is None


def test_get_tenant_kafka_bootstrap_servers_no_kafka_block():
    ctx = _make_context(tenant_kafka_config=None)
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") is None


def test_get_tenant_kafka_bootstrap_servers_returns_override():
    ctx = _make_context(tenant_kafka_config={"bootstrap_servers": "tenant-cluster:9092"})
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") == "tenant-cluster:9092"


def test_get_tenant_kafka_bootstrap_servers_coexists_with_topic_prefix():
    ctx = _make_context(
        tenant_kafka_config={"topic_prefix": "acme", "bootstrap_servers": "tenant-cluster:9092"}
    )
    assert ctx.get_tenant_kafka_bootstrap_servers("acme") == "tenant-cluster:9092"
    assert ctx.resolve_kafka_topic("orders", "acme") == "acme.orders"
