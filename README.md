# KubeMQ KEDA External Scaler

A KEDA external scaler that enables Kubernetes-native autoscaling based on KubeMQ queue depth. The scaler queries the `Waiting` message count from KubeMQ queue channels and exposes it as a KEDA metric, enabling autoscaling of queue-consuming workloads such as GPU inference workers, batch processors, and task consumers.

> **Note:** All commands below assume you are running from the `kubemq-keda/` directory.

## Architecture

```
┌──────────┐     gRPC (4 RPCs)     ┌────────────────────┐    ListQueuesChannels    ┌─────────┐
│   KEDA   │ ───────────────────── │  kubemq-keda-scaler │ ────────────────────── │  KubeMQ  │
│ Operator │                       │  (ExternalScaler)   │                        │  Broker  │
└──────────┘                       └────────────────────┘                        └─────────┘
     │                                      │
     │  ScaledObject                        │  Connection Pool
     │  metadata:                           │  (sync.Map, keyed by
     │    kubemqAddress                     │   address+tls+auth)
     │    queueName
     │    targetWaiting
     ▼
┌──────────┐
│  Target  │  ← scaled up/down based on Waiting count
│  Deploy  │
└──────────┘
```

**RPCs implemented:**
- `IsActive` — returns true when `Waiting > activationTargetWaiting`
- `StreamIsActive` — polls every 5s, pushes active status to KEDA
- `GetMetricSpec` — returns target value for `kubemq-queue-waiting` metric
- `GetMetrics` — returns current `Waiting` count from the queue

**Supported KEDA trigger types:**
- `external` — poll-based mode (default). KEDA calls `IsActive` and `GetMetrics` every `pollingInterval` seconds.
- `external-push` — push-based mode. KEDA opens a long-lived `StreamIsActive` stream. The scaler pushes updates independently from `pollingInterval`. Use this for faster scale-from-zero detection.

## Prerequisites

- Kubernetes **1.27+** (required for native gRPC probes)
- [KEDA](https://keda.sh) 2.10+ installed in the cluster
- KubeMQ broker accessible from the cluster

## Quickstart

1. **Install KEDA** (if not already installed):
   ```bash
   helm repo add kedacore https://kedacore.github.io/charts
   helm install keda kedacore/keda --namespace keda --create-namespace
   ```

2. **Deploy the scaler** via Helm:
   ```bash
   helm install kubemq-keda-scaler deploy/helm/kubemq-keda-scaler/
   ```

3. **Verify the scaler Service is running:**
   ```bash
   kubectl get svc kubemq-keda-scaler
   ```

4. **Note the scaler service FQDN** — the format is `<release-name>-kubemq-keda-scaler.<namespace>.svc.cluster.local:9090`. For the default install above: `kubemq-keda-scaler.default.svc.cluster.local:9090`.

5. **Ensure your KubeMQ broker is accessible** at the address you will use in `kubemqAddress` (e.g., `kubemq.default.svc.cluster.local:50000`).

6. **Create a ScaledObject** pointing at your deployment and queue:
   ```yaml
   apiVersion: keda.sh/v1alpha1
   kind: ScaledObject
   metadata:
     name: my-scaler
   spec:
     scaleTargetRef:
       name: my-queue-consumer
     triggers:
       - type: external
         metadata:
           scalerAddress: kubemq-keda-scaler.default.svc.cluster.local:9090
           kubemqAddress: kubemq.default.svc.cluster.local:50000
           queueName: my-queue
           targetWaiting: "10"
   ```

7. **Send messages** to the queue and observe the target deployment scaling up.

8. **Drain the queue** and observe it scaling back down (after `cooldownPeriod`).

## Installation

### Helm

```bash
helm install kubemq-keda-scaler deploy/helm/kubemq-keda-scaler/
```

With custom values:

```bash
helm install kubemq-keda-scaler deploy/helm/kubemq-keda-scaler/ \
  --set env.logLevel=debug \
  --set resources.requests.cpu=100m
```

To mount a TLS CA certificate for the KubeMQ connection:

```bash
helm install kubemq-keda-scaler deploy/helm/kubemq-keda-scaler/ \
  --set extraVolumes[0].name=certs \
  --set extraVolumes[0].secret.secretName=kubemq-ca-cert \
  --set extraVolumeMounts[0].name=certs \
  --set extraVolumeMounts[0].mountPath=/certs \
  --set extraVolumeMounts[0].readOnly=true
```

### Raw Kubernetes Manifests

> These are minimal manifests for quick testing. For production, use the Helm chart.

```bash
kubectl apply -f deploy/kubernetes/
```

### Docker

```bash
# Note: the KubeMQ broker must be reachable from the container network
docker run -p 9090:9090 kubemq/kubemq-keda-scaler:1.0.0
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `9090` | Port for the gRPC server |
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |

### ScaledObject Metadata

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `kubemqAddress` | Yes | — | KubeMQ broker address (`host:port`) |
| `queueName` | Yes | — | Queue channel name to monitor |
| `targetWaiting` | No | `10` | Target waiting messages per replica |
| `activationTargetWaiting` | No | `0` | Minimum waiting messages to activate scaling (scale from zero threshold) |
| `authToken` | No | — | KubeMQ authentication token |
| `tls` | No | `false` | Enable TLS connection (`true`, `1`, `yes`) |
| `certFile` | No | — | Path to CA certificate file (must be under `/certs/`, `/etc/ssl/`, or `/etc/pki/`) |
| `serverOverrideDomain` | No | — | TLS server name override |

### Scaler Service Address

The `scalerAddress` in your ScaledObject must point to the scaler's Kubernetes Service. The FQDN format is:

```
<service-name>.<namespace>.svc.cluster.local:9090
```

For a Helm install with release name `my-release` in namespace `keda`:
```
my-release-kubemq-keda-scaler.keda.svc.cluster.local:9090
```

## Usage Examples

### Basic Queue Autoscaling

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: my-queue-scaler
spec:
  scaleTargetRef:
    name: my-queue-consumer
  pollingInterval: 15
  cooldownPeriod: 60
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
    - type: external
      metadata:
        scalerAddress: kubemq-keda-scaler.default.svc.cluster.local:9090
        kubemqAddress: kubemq.default.svc.cluster.local:50000
        queueName: my-queue
        targetWaiting: "10"
```

### ML Inference with Scale-to-Zero

Scale GPU workers from 0 to 20 based on inference request queue depth, with a fallback of 2 replicas if the scaler is unreachable:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: ml-inference-scaler
spec:
  scaleTargetRef:
    name: gpu-inference-worker
  pollingInterval: 5
  cooldownPeriod: 300
  minReplicaCount: 0
  maxReplicaCount: 20
  fallback:
    failureThreshold: 3
    replicas: 2
  triggers:
    - type: external
      metadata:
        scalerAddress: kubemq-keda-scaler.default.svc.cluster.local:9090
        kubemqAddress: kubemq.default.svc.cluster.local:50000
        queueName: inference-requests
        targetWaiting: "5"
        activationTargetWaiting: "1"
```

### TLS with Authentication

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kubemq-trigger-auth
spec:
  secretTargetRef:
    - parameter: authToken
      name: kubemq-auth-secret
      key: token
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: secure-scaler
spec:
  scaleTargetRef:
    name: secure-consumer
  triggers:
    - type: external
      metadata:
        scalerAddress: kubemq-keda-scaler.default.svc.cluster.local:9090
        kubemqAddress: kubemq.default.svc.cluster.local:50000
        queueName: secure-queue
        targetWaiting: "10"
        tls: "true"
        certFile: /certs/ca.pem
      authenticationRef:
        name: kubemq-trigger-auth
```

### Example Files

The `deploy/examples/` directory contains ready-to-use YAML files. Apply them in this order:

| File | Description | Prerequisites |
|------|-------------|---------------|
| `trigger-auth.yaml` | TriggerAuthentication with Secret reference | Create the Secret first |
| `scaled-object-basic.yaml` | Basic queue autoscaling | — |
| `scaled-object-ml-inference.yaml` | GPU inference with scale-to-zero and fallback | — |
| `scaled-object-scale-to-zero.yaml` | Push-mode (`external-push`) scale-to-zero | — |
| `scaled-object-tls-auth.yaml` | TLS + auth token | `trigger-auth.yaml` applied first |
| `scaled-job.yaml` | ScaledJob (one Job per batch of messages) | — |

> Replace `<SCALER_SERVICE>` and `<NAMESPACE>` placeholders in the example files with your actual scaler Service name and namespace.

## Security

The gRPC server runs **plaintext** (no TLS) by design for v1. It is intended to run as a `ClusterIP` Service, accessible only within the cluster. For production, apply a `NetworkPolicy` restricting ingress to the KEDA operator namespace.

## Error Handling

The scaler maps KubeMQ errors to gRPC status codes so KEDA can use its `fallback` configuration:

| KubeMQ Error | gRPC Status | Behavior |
|-------------|-------------|----------|
| Connection refused | `Unavailable` | KEDA uses fallback replicas |
| Auth failure | `Unauthenticated` | KEDA uses fallback replicas |
| Permission denied | `PermissionDenied` | KEDA uses fallback replicas |
| Timeout | `DeadlineExceeded` | KEDA uses fallback replicas |
| Throttled | `ResourceExhausted` | KEDA uses fallback replicas |
| Not found | `NotFound` | KEDA uses fallback replicas |
| Queue not found | OK (Waiting=0) | Scale to min replicas |
| Missing metadata | `InvalidArgument` | KEDA logs error |

The scaler never returns `OK` with a fake `Waiting=0` on KubeMQ failures — it always returns a gRPC error so KEDA can apply its fallback strategy.

## Health Checks

The scaler exposes a gRPC Health service on the same port. Both liveness and readiness probes check gRPC server health only (not KubeMQ broker connectivity).

Test manually:

```bash
kubectl port-forward svc/kubemq-keda-scaler 9090:9090
grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
```

## Development

### Build

```bash
make build
```

### Test

```bash
# Unit tests
make test

# Unit tests with race detector
make test-race

# Integration tests (requires live KubeMQ)
KUBEMQ_TEST_ADDRESS=localhost:50000 make test-integration

# Coverage report
make coverage
```

### Lint

```bash
make lint
```

### Docker Build

```bash
make docker-build
```

### Proto Generation

```bash
make generate
```

## License

See [LICENSE](LICENSE) for details.
