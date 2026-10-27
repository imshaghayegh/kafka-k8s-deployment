# Production-Oriented Apache Kafka on Kubernetes with Strimzi and KRaft

A practical, reproducible deployment guide for a **3-broker, 3-controller** Apache Kafka cluster on Kubernetes using **Strimzi Operator**, **KRaft**, persistent block storage, mTLS, and ACL-based authorization.

## Versions and compatibility

- Strimzi Operator: **1.3.0**.
- Apache Kafka: **4.3.1**, with `metadataVersion: 4.3-IV0`.
- Kubernetes: **1.32–1.37** (tested compatibility advertised for Strimzi 1.3.0).
- Helm 3 and `kubectl` with administrative access.

Check [Strimzi downloads](https://strimzi.io/downloads/) and the [1.3.0 documentation](https://strimzi.io/docs/operators/1.3.0/deploying) before deploying or upgrading.

## Architecture

<img width="1672" height="941" alt="Kafka High Availability Architecture" src="https://github.com/user-attachments/assets/15fd5699-5ee9-4ddd-9ea1-06f29f0e6fa6" />

The three controllers and three brokers are **six Pods**. They can run on three sufficiently sized workers, with one broker and one controller per worker, or on separately provisioned workers for better isolation. This repository uses **hard pod anti-affinity within each pool**, so **at least three eligible schedulable Kubernetes nodes** are required. On a three-node setup, a node failure may leave a replacement Pod Pending until capacity returns, but surviving quorum/replicas should continue serving within the configured fault tolerance.

## Why KRaft and Strimzi?

- **KRaft** replaces ZooKeeper for metadata coordination.
- **Strimzi** manages lifecycle, configuration, rolling operations, users and topics using Kubernetes custom resources.
- Separate controller and broker pools make resource allocation easier to reason about.
- **Replication factor = 3** and **minimum ISR = 2** support a single-broker failure for adequately replicated and synchronized partitions; configure producers with `acks=all` (and preferably idempotence).
- TLS + mTLS + Kafka ACLs protect client connections.

## Repository structure

```text
.
├── README.md
├── .gitignore
├── scripts/
│   └── preflight.sh
└── manifests/
    ├── 01-kafka.yaml
    ├── 02-controllers.yaml
    ├── 03-brokers.yaml
    ├── 04-topic.yaml
```

## 1. Preflight checks

```bash
kubectl get nodes -o wide
kubectl get storageclass
kubectl get pv
kubectl get pvc -A
helm version
```

Confirm all of the following before installation:

1. Kubernetes version is supported by the selected Strimzi version.
2. At least three suitable worker nodes are available and schedulable, considering node taints, selectors and available resources.
3. A **dynamic, persistent block-storage StorageClass** (for example a supported Ceph RBD CSI class) is available. Prefer SSD-backed block volumes. Avoid NFS for production broker logs.
4. Worker-to-worker networking, cluster DNS, CNI NetworkPolicies and image registry egress are working.
5. At least **360 GiB** of raw PVC capacity is provisionable under the sample manifests: brokers `3 × 100Gi` plus controllers `3 × 20Gi`, excluding monitoring, snapshots and filesystem overhead.
6. Cluster time synchronization and disk performance are acceptable for expected throughput.


For capacity planning, include replication overhead, retention, partition count, expected message size, peak ingress, consumer lag, disk IOPS, growth and recovery headroom. The supplied CPU, memory and PVC values are examples, **not workload-tested production sizing**.

## 2. Install the Strimzi operator

```bash
kubectl create namespace kafka

helm upgrade --install strimzi-cluster-operator \
  oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 1.3.0 \
  --namespace kafka \
  --set watchAnyNamespace=false

kubectl rollout status deployment/strimzi-cluster-operator -n kafka --timeout=300s
kubectl get crd | grep kafka.strimzi.io
kubectl get pods -n kafka
```

The operator is installed in, and watches, the `kafka` namespace in this example. Strimzi installation requires Kubernetes permissions to manage CRDs and RBAC.

## 3. Configure storage and deploy Kafka

Edit `manifests/02-controllers.yaml` and `manifests/03-brokers.yaml` to replace the storage-class placeholder. Review CPU, memory, node placement and disk size.

```bash
kubectl apply -f manifests/02-controllers.yaml
kubectl apply -f manifests/03-brokers.yaml
kubectl apply -f manifests/01-kafka.yaml

kubectl get kafka,kafkanodepool -n kafka
kubectl get pods -n kafka -o wide
kubectl get pvc -n kafka
kubectl wait kafka/prod-kafka -n kafka --for=condition=Ready --timeout=900s
```

The `Kafka` resource defines the version, listeners, authorization and replication defaults. `KafkaNodePool` resources define separate controller/broker Pods and storage.

> PVCs use `deleteClaim: false` to reduce the chance of accidental data deletion when the Kafka resources are deleted. This **does not substitute for backups** and does not override every possible CSI reclaim or infrastructure failure behavior.

## 4. Understand the listener and network ports

| Port | Purpose | Exposure |
|---|---|---|
| 9093 | Sample TLS/mTLS client listener | Internal Kubernetes Service |
| 9090 | Controller communication (Strimzi-managed) | Internal Kafka component traffic |
| 9091 | Broker replication / internal communication (Strimzi-managed) | Internal Kafka component traffic |
| 9404 | Kafka metrics, if configured | Monitoring only |

# Kafka Metrics → Mimir

Kafka metrics are exported by the Strimzi-managed Kafka Exporter, scraped by Fluent Bit, and forwarded to Grafana Mimir using Prometheus Remote Write.

```text
Kafka (prod-kafka)
  └── Kafka Exporter :9404/metrics
        └── Fluent Bit (prometheus_scrape)
              └── Mimir Gateway /api/v1/push (tenant: kafka-monitoring)
```

## Components

| Component | Configuration |
| --- | --- |
| Kafka | `prod-kafka` (Strimzi, namespace `kafka`) |
| Kafka Exporter | Strimzi-managed, port `9404`, topics/groups `.*` |
| Fluent Bit | Single-replica Deployment, scrape interval `15s` |
| Mimir | `mimir-gateway.mimir.svc.cluster.local:80`, `/api/v1/push` |
| Tenant header | `X-Scope-OrgID: kafka-monitoring` |

## Kafka Exporter

The following block is part of the existing **Kafka** custom resource (`spec.kafkaExporter`), not a standalone Deployment:

```yaml
spec:
  kafkaExporter:
    topicRegex: ".*"
    groupRegex: ".*"
    resources:
      requests:
        cpu: 100m
        memory: 128Mi
      limits:
        memory: 512Mi
```

Exporter service endpoint:

```text
http://kafka-exporter-metrics.kafka.svc.cluster.local:9404/metrics
```

## Fluent Bit

The repository includes:

```text
Fluent-bit/
├── 01-fluent-bit-configmap.yaml
└── 02-fluent-bit-deployment.yaml
```

Fluent Bit reads the exporter endpoint with `prometheus_scrape` and sends metric samples to Mimir using `prometheus_remote_write`. The Mimir tenant is selected by the `X-Scope-OrgID` header. The Deployment uses an internal-registry image placeholder to match the cluster's image distribution approach.

```bash
kubectl apply -f k8s/01-fluent-bit-configmap.yaml
kubectl apply -f k8s/02-fluent-bit-deployment.yaml
```

## Verification

```bash
kubectl get kafka prod-kafka -n kafka
kubectl get svc -n kafka prod-kafka-kafka-exporter
kubectl get deploy,pods -n kafka -l app=kafka-metrics-fluent-bit
kubectl logs -n kafka deploy/kafka-metrics-fluent-bit --tail=100
```

Sample metric queries in Grafana Explore (Mimir datasource):

```promql
kafka_brokers
kafka_topic_partitions
kafka_consumergroup_lag
```

## References

- [Strimzi downloads / compatibility](https://strimzi.io/downloads/)
- [Strimzi 1.3.0 deployment guide](https://strimzi.io/docs/operators/1.3.0/deploying)
- [Official Strimzi Kafka persistent example](https://github.com/strimzi/strimzi-kafka-operator/blob/main/examples/kafka/kafka-persistent.yaml)
- [Official Strimzi TLS authorization example](https://github.com/strimzi/strimzi-kafka-operator/blob/main/examples/security/tls-auth/kafka.yaml)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [Strimzi Kafka Exporter configuration](https://strimzi.io/docs/operators/in-development/full/configuring.html)
- [Fluent Bit Prometheus scrape input](https://docs.fluentbit.io/manual/data-pipeline/inputs/prometheus-scrape-metrics)
- [Fluent Bit Prometheus Remote Write output](https://docs.fluentbit.io/manual/data-pipeline/outputs/prometheus-remote-write)

