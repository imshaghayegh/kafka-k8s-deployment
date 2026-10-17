# Production-Oriented Apache Kafka on Kubernetes with Strimzi and KRaft

A practical, reproducible deployment guide for a **3-broker, 3-controller** Apache Kafka cluster on Kubernetes using **Strimzi Operator**, **KRaft**, persistent block storage, mTLS, and ACL-based authorization.

> **Scope:** A production-oriented baseline, not a turnkey production certification. You must validate Kubernetes compatibility, available storage, capacity, failure domains, networking, backup, and security before deploying real workloads.

## Versions and compatibility

- Strimzi Operator: **1.3.0** (release available as of October 17, 2026).
- Apache Kafka: **4.3.1**, with `metadataVersion: 4.3-IV0`.
- Kubernetes: **1.32–1.37** (tested compatibility advertised for Strimzi 1.3.0).
- Helm 3 and `kubectl` with administrative access.

Check [Strimzi downloads](https://strimzi.io/downloads/) and the [1.3.0 documentation](https://strimzi.io/docs/operators/1.3.0/deploying) before deploying or upgrading.

## Architecture

```text
                Applications / Kafka Clients
                           |
                     TLS + mTLS :9093
                           |
       prod-kafka-kafka-bootstrap.kafka.svc:9093
                           |
                +----------+----------+
                |          |          |
             Broker 0   Broker 1   Broker 2
               PVC        PVC        PVC
                |          |          |
                +--- Kafka replicated logs --+

                KRaft metadata quorum
          Controller 0 / Controller 1 / Controller 2
                PVC          PVC          PVC

              Strimzi Cluster Operator
        (reconciles Kafka and KafkaNodePool CRs)
```

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
    ├── 05-user.yaml
    └── 06-client-pod.yaml
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

**Important:** Replace `CHANGE_ME_BLOCK_STORAGE_CLASS` in both node pool YAML files with a real block-storage class returned by `kubectl get storageclass`. Do not apply the manifests with this placeholder unchanged.

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

### Resource defaults

| Pool | Pods | CPU request each | Memory request/limit each | PVC each |
|---|---:|---:|---|---:|
| brokers | 3 | 2 cores | 4 / 6 GiB | 100 GiB |
| controllers | 3 | 0.5 cores | 1 / 2 GiB | 20 GiB |

To prevent uncontrolled eviction, configure appropriate node reservations, PodDisruptionBudgets (if applicable and compatible with operator-managed resources), and planned maintenance workflows. Never drain multiple controller hosts concurrently without verifying quorum health.

## 4. Create a topic and user

```bash
kubectl apply -f manifests/04-topic.yaml
kubectl apply -f manifests/05-user.yaml

kubectl get kafkatopic,kafkauser -n kafka
kubectl get secret orders-app -n kafka
```

- `orders` topic: six partitions, three replicas, seven-day time retention and minimum ISR two.
- `orders-app`: mTLS credentials with topic Read/Write/Describe and consumer-group Read access for groups beginning `orders-consumer`.
- The secret is generated by the Strimzi User Operator. **Do not commit secrets, certificates, passwords or private keys to Git.**

## 5. Verify producer / consumer messaging

Create a temporary Kafka client Pod with the user's Kubernetes secret and the Strimzi cluster CA mounted read-only:

```bash
kubectl apply -f manifests/06-client-pod.yaml
kubectl wait --for=condition=Ready pod/kafka-client -n kafka --timeout=180s
kubectl exec -it -n kafka kafka-client -- /bin/sh
```

Inside the client Pod, create client TLS properties:

```bash
cat > /tmp/client.properties <<EOF2
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/ca/ca.crt
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/user/user.p12
ssl.keystore.password=$(cat /etc/kafka/user/user.password)
EOF2

cd /opt/kafka
bin/kafka-topics.sh \
  --bootstrap-server prod-kafka-kafka-bootstrap.kafka.svc:9093 \
  --command-config /tmp/client.properties \
  --describe --topic orders
```

Produce messages:

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server prod-kafka-kafka-bootstrap.kafka.svc:9093 \
  --topic orders \
  --producer.config /tmp/client.properties \
  --producer-property acks=all
```

Enter one or more messages, then press Ctrl+C. Consume in a separate exec session (or after exiting producer):

```bash
kubectl exec -it -n kafka kafka-client -- /bin/sh
cd /opt/kafka
bin/kafka-console-consumer.sh \
  --bootstrap-server prod-kafka-kafka-bootstrap.kafka.svc:9093 \
  --topic orders \
  --group orders-consumer-test \
  --consumer.config /tmp/client.properties \
  --from-beginning
```

**Note:** `/tmp/client.properties` is created inside the Pod, not on your local workstation. In a newly created shell into the same Pod it persists, but if you recreate the Pod, run the properties setup again. In some Kafka container images the Kafka scripts are elsewhere; use `command -v kafka-topics.sh` or inspect the image if `/opt/kafka` is absent.

**Clean up the test client:**

```bash
kubectl delete -f manifests/06-client-pod.yaml
```

## 6. Understand the listener and network ports

| Port | Purpose | Exposure |
|---|---|---|
| 9093 | Sample TLS/mTLS client listener | Internal Kubernetes Service |
| 9090 | Controller communication (Strimzi-managed) | Internal Kafka component traffic |
| 9091 | Broker replication / internal communication (Strimzi-managed) | Internal Kafka component traffic |
| 9404 | Kafka metrics, if configured | Monitoring only |

The example does **not** expose Kafka outside Kubernetes. Kafka clients first contact a bootstrap server and subsequently connect to the individual broker addresses returned in metadata. External exposure therefore requires a correctly configured `loadbalancer`, `nodeport`, `ingress` or other supported listener with **reachable advertised addresses for each broker**; a generic HTTP Ingress to only the bootstrap Service is not sufficient.

## 7. Monitoring and alerts

Integrate metrics with Prometheus/Grafana using Strimzi-supported metrics configuration and a `PodMonitor`/`ServiceMonitor` appropriate for the installed Prometheus Operator. This repository intentionally does not deploy an unverified metrics configuration or expose a JMX port by default.

Recommended dashboard and alerts:

| Metric / condition | Why monitor it |
|---|---|
| Under-replicated partitions | Loss of replica synchronization |
| Offline partitions | Unavailable data partitions |
| Active controller/quorum health | Metadata availability |
| Consumer lag | Consumers failing to keep up |
| PVC free space and growth | Avoid disk exhaustion |
| Broker CPU, memory and restarts | Saturation or instability |
| Produce/fetch request latency | Client-facing performance |
| Reassignment progress | Recovery and balancing capacity |

Set actionable alert thresholds for actual traffic patterns. Track user-facing produce/consume success in addition to cluster internals.

## 8. Failure drills

Perform these **only in a test environment or an approved maintenance window**:

1. Start a continuous producer with `acks=all` and a consumer.
2. Record partition ISR and cluster health.
3. Gracefully restart one broker Pod **one at a time** and observe recovery.
4. Verify that other in-sync replicas continue serving eligible partitions.
5. Confirm the restarted broker rejoins the ISR before repeating.
6. Separately test controller failure and loss/recovery of a worker node.

Do not delete PVCs or remove multiple controllers. Three KRaft controllers require a **majority of two controllers** to remain available. RF=3 / minISR=2 tolerates one unavailable replica **only while the remaining replicas are in sync**.

Useful diagnostics:

```bash
kubectl get pods -n kafka -o wide
kubectl get kafka prod-kafka -n kafka -o yaml
kubectl get kafkanodepool -n kafka
kubectl get pvc -n kafka
kubectl describe pod -n kafka <POD_NAME>
kubectl logs -n kafka deployment/strimzi-cluster-operator --tail=200
kubectl get events -n kafka --sort-by=.lastTimestamp
```

## 9. Operational runbook and troubleshooting

| Symptom | Checks |
|---|---|
| Pods Pending | PVC binding, node resources, anti-affinity, taints |
| PVC Pending | StorageClass name, CSI health, quota, available capacity |
| Cluster not Ready | `kubectl describe kafka`, operator logs, Pod events |
| Client TLS fails | CA, user certificate/PKCS12, hostname and listener |
| `TopicAuthorizationException` | KafkaUser ACLs and authenticated principal |
| Produce errors with min ISR | Broker health, ISR size, disk/network saturation |
| Consumer lag rising | Consumer count, partitions, downstream performance |
| Disk nearly full | Retention, incoming rate, rebalancing, partition distribution |

Use `kubectl get kafka prod-kafka -n kafka -o yaml` to inspect status conditions. Review logs and root causes before restarting anything.

## 10. Backups, upgrades, and production hardening

- Kafka replication is **not backup**. Plan topic-data recovery, off-cluster replication or a supported backup/restore solution and test restoration.
- Back up critical Kubernetes manifests, user definitions and external secrets/PKI configuration securely.
- Verify partition layout, load balancing, rack/zone placement, broker disk performance and sufficient recovery capacity.
- Keep client traffic encrypted/authenticated. Restrict namespaces and client egress/ingress with CNI-enforced NetworkPolicies.
- Treat cluster CA and private keys as sensitive; define certificate rotation policy.
- Plan upgrades by reading the **specific Strimzi release notes and Kafka upgrade matrix**. Test operator, broker and metadata upgrades in staging; do not independently bump all versions at once.
- Review production retention and `message.max.bytes`, broker requests/limits, client retries, timeouts, idempotence, `acks=all`, and partition/replica placement.
- Use GitOps (Argo CD) to reconcile CR manifests, while treating the shared Strimzi operator and CRD upgrade ordering carefully. Do not let two tools fight over the same resource.

## 11. Safe change workflow

```bash
# Inspect the intended diff before deploying
kubectl diff -f manifests/02-controllers.yaml
kubectl diff -f manifests/03-brokers.yaml
kubectl diff -f manifests/01-kafka.yaml

# Apply only after reviewing effects on availability and storage
kubectl apply -f manifests/02-controllers.yaml
kubectl apply -f manifests/03-brokers.yaml
kubectl apply -f manifests/01-kafka.yaml
```

Changing storage capacity, pool membership, roles or listener endpoints is not necessarily a simple rolling update. Follow documented Strimzi procedures for each change. Be especially cautious with PVC migrations and partition reassignment.

## References

- [Strimzi downloads / compatibility](https://strimzi.io/downloads/)
- [Strimzi 1.3.0 deployment guide](https://strimzi.io/docs/operators/1.3.0/deploying)
- [Official Strimzi Kafka persistent example](https://github.com/strimzi/strimzi-kafka-operator/blob/main/examples/kafka/kafka-persistent.yaml)
- [Official Strimzi TLS authorization example](https://github.com/strimzi/strimzi-kafka-operator/blob/main/examples/security/tls-auth/kafka.yaml)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)

## Disclaimer

This is a reference implementation. Adapt and validate it for your environment before production deployment. Nothing in this repository has been executed against your Kubernetes cluster.
