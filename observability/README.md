# Enterprise metrics examples

See [the metrics guide](../docs/monitoring.md) for licensing, configuration,
metric definitions, sampling limitations, PromQL examples, and Kubernetes setup.
Metrics require GizmoSQL v1.39.0 or newer and the `metrics` license entitlement.
They are disabled by default and independent of SQL instrumentation.

This directory provides a local Compose example, a Prometheus scrape
configuration, eight alert rules, a provisioned Grafana dashboard, and an
example PodMonitor. The Compose example uses plaintext Flight SQL for local
testing. Configure TLS and deployment-specific network access for production.

After the v1.39.0 image is published:

```bash
export GIZMOSQL_LICENSE_FILE=/private/path/to/metrics-license.txt
export GIZMOSQL_PASSWORD=choose-a-local-test-password
export GRAFANA_ADMIN_PASSWORD=choose-a-local-dashboard-password
docker compose -f observability/compose.yml up -d
```

Open Grafana at http://localhost:3000, sign in as `admin` with the password you
configured, and select the GizmoSQL dashboard. Prometheus is available at
http://localhost:9090. The metrics port is exposed only inside the Compose
network; it is not published on the host.

The dashboard has `Namespace` and `Pod` variables (from the `namespace`/`pod`
labels Prometheus adds to PodMonitor scrapes) next to `Instance`; on a
non-Kubernetes deployment they match everything. Legends are prefixed with the
pod name so replicas stay distinct. To load it into a kube-prometheus-stack
Grafana, put the JSON in a ConfigMap labelled `grafana_dashboard: "1"` in the
Grafana namespace (the dashboard sidecar picks it up).

Adjust the PodMonitor's namespace and selectors to match your deployment and
Prometheus Operator configuration. Its named `metrics` port must refer to the
container's configured metrics port. Applying a PodMonitor alone does not make
an arbitrary Prometheus installation discover it.

Remote metastore health probes are deferred from v1.39.0. The dashboard and
alerts do not depend on them.
