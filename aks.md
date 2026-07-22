# AKS Cluster Architecture

## Overview

This document defines the AKS infrastructure for deploying the Config-Driven Data Pipeline across four environments: Dev, Test, Pre-Production, and Production.

### Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Executor** | KubernetesExecutor | Dynamic task pods, cost-efficient for bursty workloads |
| **Node Pools** | Dedicated pools (Pre/Prod), Combined (Dev/Test) | Isolation where it matters, cost savings where it doesn't |
| **Service Mesh** | ISTIO (AKS managed) | mTLS encryption, AuthorizationPolicy, observability |
| **Kafka Deployment** | Strimzi Operator (separate repo) | KRaft mode, CRD-based management, mesh integrated |

### Workload Characteristics

| Metric | Current | 2-Year Projection |
|--------|---------|-------------------|
| DAGs/Data Factories | 50 | 100-150 |
| Events per day | 5K-10K | 50K-100K |
| Message throughput | <2000 events/s peak | Scales with DAGs |
| Average message size | ~2KB | ~2-4KB |

---

## Node Pool Architecture

### Dev Cluster

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DEV CLUSTER                                    │
├─────────────────┬────────┬──────────┬───────────────────────────────────────┤
│ Node Pool       │ Nodes  │ VM Size  │ Workloads                             │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ system          │ 1      │ B4als    │ kube-system, ISTIO, observability     │
│                 │        │ 4vCPU    │                                       │
│                 │        │ 8GB RAM  │                                       │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ workload        │ 1      │ B8als    │ Kafka + Airflow (combined)            │
│                 │        │ 8vCPU    │                                       │
│                 │        │ 16GB RAM │                                       │
├─────────────────┴────────┴──────────┴───────────────────────────────────────┤
│ Total: 2 nodes | 12 vCPU / 24 GB | ~£180/month | £2,160/year                │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Taints & Tolerations:**
- `system`: `CriticalAddonsOnly=true:NoSchedule`
- `workload`: No taint (accepts all workloads)

---

### Test Cluster

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TEST CLUSTER                                    │
├─────────────────┬────────┬──────────┬───────────────────────────────────────┤
│ Node Pool       │ Nodes  │ VM Size  │ Workloads                             │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ system          │ 1      │ B4als    │ kube-system, ISTIO, observability     │
│                 │        │ 4vCPU    │                                       │
│                 │        │ 8GB RAM  │                                       │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ workload        │ 2      │ B8als    │ Kafka + Airflow + task pods           │
│                 │        │ 8vCPU    │                                       │
│                 │        │ 16GB RAM │                                       │
├─────────────────┴────────┴──────────┴───────────────────────────────────────┤
│ Total: 3 nodes | 20 vCPU / 40 GB | ~£270/month | £3,240/year                │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Taints & Tolerations:**
- `system`: `CriticalAddonsOnly=true:NoSchedule`
- `workload`: No taint (accepts all workloads)

---

### Pre-Production Cluster

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PRE CLUSTER                                     │
├─────────────────┬────────┬──────────┬───────────────────────────────────────┤
│ Node Pool       │ Nodes  │ VM Size  │ Workloads                             │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ system          │ 1      │ B4als    │ kube-system, ISTIO, observability     │
│                 │        │ 4vCPU    │                                       │
│                 │        │ 8GB RAM  │                                       │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ kafka           │ 2      │ B4als    │ Kafka brokers (3, anti-affinity)      │
│                 │        │ 4vCPU    │                                       │
│                 │        │ 8GB RAM  │                                       │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ airflow         │ 2      │ B8als    │ Scheduler, webserver, task pods       │
│                 │        │ 8vCPU    │                                       │
│                 │        │ 16GB RAM │                                       │
├─────────────────┴────────┴──────────┴───────────────────────────────────────┤
│ Total: 5 nodes | 28 vCPU / 56 GB | ~£450/month | £5,400/year                │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Taints & Tolerations:**
- `system`: `CriticalAddonsOnly=true:NoSchedule`
- `kafka`: `workload=kafka:NoSchedule`
- `airflow`: `workload=airflow:NoSchedule`

---

### Production Cluster

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PROD CLUSTER                                    │
├─────────────────┬────────┬──────────┬───────────────────────────────────────┤
│ Node Pool       │ Nodes  │ VM Size  │ Workloads                             │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ system          │ 2      │ B4als    │ kube-system, ISTIO HA, observability  │
│                 │        │ 4vCPU    │                                       │
│                 │        │ 8GB RAM  │                                       │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ kafka           │ 3      │ B4als    │ Kafka brokers (3, 1 per node)         │
│                 │        │ 4vCPU    │                                       │
│                 │        │ 8GB RAM  │                                       │
├─────────────────┼────────┼──────────┼───────────────────────────────────────┤
│ airflow         │ 3      │ B8als    │ Scheduler HA, webserver, task pods    │
│                 │        │ 8vCPU    │                                       │
│                 │        │ 16GB RAM │                                       │
├─────────────────┴────────┴──────────┴───────────────────────────────────────┤
│ Total: 8 nodes | 44 vCPU / 88 GB | ~£700/month | £8,400/year                │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Taints & Tolerations:**
- `system`: `CriticalAddonsOnly=true:NoSchedule`
- `kafka`: `workload=kafka:NoSchedule`
- `airflow`: `workload=airflow:NoSchedule`

---

## Namespace Layout

| Namespace | Purpose | Node Pool | ISTIO Injection |
|-----------|---------|-----------|-----------------|
| `kube-system` | Kubernetes core components | system | No |
| `kube-public` | Public cluster resources | system | No |
| `kube-node-lease` | Node heartbeats | system | No |
| `default` | Default namespace (unused) | - | No |
| `aks-istio-system` | ISTIO control plane | system | No |
| `aks-istio-ingress` | ISTIO ingress gateway | system | No |
| `aks-istio-egress` | ISTIO egress gateway | system | No |
| `observability` | Prometheus, Grafana, Jaeger, Kiali | system | Yes |
| `kafka` | Strimzi operator + Kafka brokers | kafka/workload | Yes |
| `airflow` | Scheduler, webserver, triggerer | airflow/workload | Yes |

### Namespace YAML Examples

```yaml
# Kafka namespace with ISTIO injection
apiVersion: v1
kind: Namespace
metadata:
  name: kafka
  labels:
    istio-injection: enabled
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted

---
# Airflow namespace with ISTIO injection
apiVersion: v1
kind: Namespace
metadata:
  name: airflow
  labels:
    istio-injection: enabled
    pod-security.kubernetes.io/enforce: baseline
    pod-security.kubernetes.io/audit: restricted
```

---

## Resource Allocations

### Kafka Resources

| Env | Brokers | Request (per broker) | Limit (per broker) | Storage | Retention |
|-----|---------|---------------------|-------------------|---------|-----------|
| Dev | 1 | 1 vCPU / 2 GB | 2 vCPU / 4 GB | 20 GB | 1 day |
| Test | 2 | 1 vCPU / 2 GB | 2 vCPU / 4 GB | 30 GB | 1 day |
| Pre | 3 | 1.5 vCPU / 3 GB | 2 vCPU / 6 GB | 50 GB | 1 day |
| Prod | 3 | 2 vCPU / 4 GB | 4 vCPU / 8 GB | 100 GB | 7 days |

**Kafka Topics:**

| Topic | Partitions | Replication Factor | Purpose |
|-------|------------|-------------------|---------|
| `market-data` | 3 | env-dependent | Market data events |
| `pipeline-events` | 3 | env-dependent | DAG/task lifecycle events |
| `dq-metrics` | 3 | env-dependent | Data quality validation results |
| `openlineage.events` | 3 | env-dependent | Data lineage events |

**Replication Factor by Environment:**
- Dev: 1 (single broker)
- Test: 2
- Pre/Prod: 3

---

### Airflow Resources (KubernetesExecutor)

| Env | Component | Replicas | Request | Limit |
|-----|-----------|----------|---------|-------|
| **Dev** | Scheduler | 1 | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| | Webserver | 1 | 0.25 vCPU / 512 MB | 0.5 vCPU / 1 GB |
| | Triggerer | 1 | 0.25 vCPU / 512 MB | 0.5 vCPU / 1 GB |
| | Task Pod (default) | dynamic | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| **Test** | Scheduler | 1 | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| | Webserver | 1 | 0.25 vCPU / 512 MB | 0.5 vCPU / 1 GB |
| | Triggerer | 1 | 0.25 vCPU / 512 MB | 0.5 vCPU / 1 GB |
| | Task Pod (default) | dynamic | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| **Pre** | Scheduler | 2 | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| | Webserver | 2 | 0.25 vCPU / 512 MB | 0.5 vCPU / 1 GB |
| | Triggerer | 2 | 0.25 vCPU / 512 MB | 0.5 vCPU / 1 GB |
| | Task Pod (default) | dynamic | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| **Prod** | Scheduler | 2 | 1 vCPU / 2 GB | 2 vCPU / 4 GB |
| | Webserver | 2 | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| | Triggerer | 2 | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| | Task Pod (default) | dynamic | 1 vCPU / 2 GB | 2 vCPU / 4 GB |

**KubernetesExecutor Benefits:**
- Task pods spawn on-demand, terminate after completion
- No idle workers consuming resources
- Different tasks can have different resource profiles via `pod_override`
- Scales naturally with DAG concurrency
- Cost-efficient for bursty workloads

---

## Security Architecture

### ISTIO Service Mesh

All workload pods run inside the ISTIO mesh with automatic mTLS encryption.

```yaml
# PeerAuthentication - enforce mTLS in kafka namespace
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: kafka-mtls
  namespace: kafka
spec:
  mtls:
    mode: STRICT

---
# AuthorizationPolicy - only allow airflow namespace to access Kafka
apiVersion: security.istio.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: kafka-access
  namespace: kafka
spec:
  selector:
    matchLabels:
      strimzi.io/cluster: kafka-cluster
  rules:
    - from:
        - source:
            namespaces: ["airflow", "kafka"]  # kafka for inter-broker
      to:
        - operation:
            ports: ["9092", "9093"]  # client + controller
```

### Kafka Authentication (SASL_SCRAM)

Even with ISTIO mTLS, SASL_SCRAM provides application-level identity.

**KafkaUser with Least-Privilege ACLs:**

```yaml
# Producer-only user (Airflow publishing events)
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: airflow-producer
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: pipeline-events
          patternType: literal
        operations: [Write, Describe]
        host: "*"
      - resource:
          type: topic
          name: dq-metrics
          patternType: literal
        operations: [Write, Describe]
        host: "*"
```

### Network Policies

```yaml
# Isolate Strimzi operator pod
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: strimzi-operator-isolation
  namespace: kafka
spec:
  podSelector:
    matchLabels:
      strimzi.io/kind: cluster-operator
  policyTypes:
    - Ingress
    - Egress
  ingress: []     # No inbound traffic needed
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: kafka
    - to:
        - namespaceSelector: {}
          podSelector:
            matchLabels:
              component: kube-apiserver
```

### RBAC for Cross-Namespace Secret Access

Strimzi creates KafkaUser secrets in `kafka` namespace. Airflow needs access.

**Option 1: Secret Replication (Recommended)**

Use Kubernetes Replicator or External Secrets Operator to sync secrets.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: airflow
  annotations:
    replicator.v1.mittwald.de/replicate-from: kafka/airflow-producer
type: Opaque
```

**Option 2: Cross-Namespace RBAC**

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kafka-secret-reader
  namespace: kafka
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["airflow-producer"]
    verbs: ["get"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-kafka-secret-access
  namespace: kafka
subjects:
  - kind: ServiceAccount
    name: airflow-worker
    namespace: airflow
roleRef:
  kind: Role
  name: kafka-secret-reader
  apiGroup: rbac.authorization.k8s.io
```

---

## Observability

### Components (in `observability` namespace)

| Component | Purpose | Resource Allocation |
|-----------|---------|---------------------|
| Prometheus | Metrics collection | 1 vCPU / 2 GB |
| Grafana | Dashboards & visualization | 0.5 vCPU / 1 GB |
| Jaeger | Distributed tracing | 0.5 vCPU / 1 GB |
| Kiali | Service mesh observability | 0.25 vCPU / 512 MB |

### Kafka Metrics (PodMonitor)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: kafka-metrics
  namespace: kafka
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      strimzi.io/cluster: kafka-cluster
      strimzi.io/kind: Kafka
  podMetricsEndpoints:
    - port: tcp-prometheus
      path: /metrics
```

### Key Metrics to Monitor

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| `kafka_controller_kafkacontroller_activecontrollercount` | != 1 | Must have exactly 1 active controller |
| `kafka_server_replicamanager_underreplicatedpartitions` | > 0 | Indicates broker issues |
| `kafka_server_brokertopicmetrics_messagesin_total` | Baseline deviation | Message throughput |
| `kafka_network_requestmetrics_requestspersec` | Capacity limit | Request rate |
| `kafka_log_log_size` | 80% of disk | Disk usage |

### Audit Logging

Kafka authorizer logs ship to Azure Monitor via Container Insights.

```yaml
# Kafka CRD logging config
spec:
  kafka:
    logging:
      type: inline
      loggers:
        kafka.authorizer.logger: INFO
        kafka.network.RequestChannel: INFO
```

**Azure Monitor KQL Query:**
```kql
ContainerLog
| where ContainerName == "kafka"
| where LogEntry contains "authorizer"
| project TimeGenerated, LogEntry
```

---

## Cost Summary

| Environment | Nodes | Monthly Cost | Annual Cost |
|-------------|-------|--------------|-------------|
| Dev | 2 | £180 | £2,160 |
| Test | 3 | £270 | £3,240 |
| Pre | 5 | £450 | £5,400 |
| Prod | 8 | £700 | £8,400 |
| **Total** | **18** | **£1,600** | **£19,200** |

### Comparison with Current ADF

| Environment | ADF + VNet (Annual) | AKS (Annual) | Savings |
|-------------|---------------------|--------------|---------|
| Dev | £29,298 | £2,160 | £27,138 |
| Test | £34,425 | £3,240 | £31,185 |
| Pre | £37,662 | £5,400 | £32,262 |
| Prod | £68,281 | £8,400 | £59,881 |
| **Total** | **£169,666** | **£19,200** | **£150,466 (88%)** |

---

## Scaling Strategy

### Horizontal Scaling (50 → 150 DAGs)

| Growth Stage | DAGs | Action | Cost Impact |
|--------------|------|--------|-------------|
| Now | 50 | Base allocation | - |
| +50% | 75 | Task pods auto-scale (KubernetesExecutor) | Minimal |
| +100% | 100 | Enable node pool autoscaler (1→2 airflow nodes) | +£90/month |
| +200% | 150 | Add airflow node, increase scheduler resources | +£180/month |

### Node Pool Autoscaling Configuration

```yaml
# AKS node pool autoscaler settings
nodePoolProfiles:
  - name: airflow
    enableAutoScaling: true
    minCount: 2
    maxCount: 5
    mode: User
  - name: kafka
    enableAutoScaling: false  # Kafka prefers static sizing
    count: 3
    mode: User
```

### Kafka Scaling

Kafka scales **vertically** (more resources per broker) before **horizontally** (more brokers).

| Throughput | Action |
|------------|--------|
| <10K events/s | Current sizing sufficient |
| 10K-50K events/s | Increase broker CPU/memory limits |
| >50K events/s | Add brokers, increase partitions |

---

## Deployment Checklist

### Pre-Deployment

- [ ] AKS cluster created with node pools
- [ ] ISTIO add-on enabled
- [ ] Azure Container Registry accessible
- [ ] Azure Monitor Container Insights enabled
- [ ] Prometheus/Grafana deployed to observability namespace

### Kafka Deployment

- [ ] Create `kafka` namespace with ISTIO injection
- [ ] Deploy Strimzi Operator via Helm
- [ ] Deploy Kafka cluster CRD
- [ ] Verify broker pods running
- [ ] Create KafkaTopics
- [ ] Create KafkaUsers with ACLs
- [ ] Configure PodMonitor for Prometheus
- [ ] Import Grafana dashboard

### Airflow Deployment

- [ ] Create `airflow` namespace with ISTIO injection
- [ ] Deploy PostgreSQL (or use Azure Database for PostgreSQL)
- [ ] Deploy Airflow via Helm with KubernetesExecutor
- [ ] Configure Kafka connection (secrets, bootstrap servers)
- [ ] Verify scheduler, webserver, triggerer running
- [ ] Test DAG execution with task pod creation

### Validation

```bash
# Verify Kafka brokers
kubectl get kafka -n kafka
kubectl get pods -n kafka -l strimzi.io/kind=Kafka

# Test Kafka connectivity from Airflow namespace
kubectl run kafka-test --rm -it -n airflow \
  --image=bitnami/kafka:latest \
  -- kafka-topics.sh \
    --bootstrap-server kafka-cluster-kafka-bootstrap.kafka:9092 \
    --list

# Verify ISTIO mTLS
istioctl authn tls-check kafka-cluster-kafka-0.kafka

# Check Prometheus targets
kubectl port-forward -n observability svc/prometheus 9090:9090
# Visit http://localhost:9090/targets
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AKS CLUSTER                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                         ISTIO SERVICE MESH                              ││
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              ││
│  │  │   AIRFLOW    │    │    KAFKA     │    │ OBSERVABILITY│              ││
│  │  │  NAMESPACE   │    │  NAMESPACE   │    │  NAMESPACE   │              ││
│  │  │              │    │              │    │              │              ││
│  │  │ ┌──────────┐ │    │ ┌──────────┐ │    │ ┌──────────┐ │              ││
│  │  │ │Scheduler │ │    │ │ Broker 1 │ │    │ │Prometheus│ │              ││
│  │  │ │ (HA x2)  │ │    │ └──────────┘ │    │ └──────────┘ │              ││
│  │  │ └──────────┘ │    │ ┌──────────┐ │    │ ┌──────────┐ │              ││
│  │  │ ┌──────────┐ │    │ │ Broker 2 │ │    │ │ Grafana  │ │              ││
│  │  │ │Webserver │ │───▶│ └──────────┘ │    │ └──────────┘ │              ││
│  │  │ │ (HA x2)  │ │mTLS│ ┌──────────┐ │    │ ┌──────────┐ │              ││
│  │  │ └──────────┘ │    │ │ Broker 3 │ │    │ │  Jaeger  │ │              ││
│  │  │ ┌──────────┐ │    │ └──────────┘ │    │ └──────────┘ │              ││
│  │  │ │Task Pods │ │    │ ┌──────────┐ │    │ ┌──────────┐ │              ││
│  │  │ │(dynamic) │ │    │ │ Strimzi  │ │    │ │  Kiali   │ │              ││
│  │  │ └──────────┘ │    │ │ Operator │ │    │ └──────────┘ │              ││
│  │  └──────────────┘    │ └──────────┘ │    └──────────────┘              ││
│  │         │            └──────────────┘            │                      ││
│  │         │                   │                    │                      ││
│  │         └───────────────────┼────────────────────┘                      ││
│  │                             │                                           ││
│  │                    ┌────────▼────────┐                                  ││
│  │                    │  Azure Monitor  │                                  ││
│  │                    │ (Container      │                                  ││
│  │                    │  Insights)      │                                  ││
│  │                    └─────────────────┘                                  ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │  SYSTEM POOL    │  │   KAFKA POOL    │  │  AIRFLOW POOL   │             │
│  │  B4als (1-2)    │  │  B4als (2-3)    │  │  B8als (2-3)    │             │
│  │  kube-system    │  │  kafka brokers  │  │  airflow pods   │             │
│  │  ISTIO          │  │  strimzi op     │  │  task pods      │             │
│  │  observability  │  │                 │  │                 │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## References

- [Strimzi Documentation](https://strimzi.io/documentation/)
- [AKS Best Practices](https://learn.microsoft.com/en-us/azure/aks/best-practices)
- [ISTIO on AKS](https://learn.microsoft.com/en-us/azure/aks/istio-about)
- [Apache Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [KubernetesExecutor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html)
