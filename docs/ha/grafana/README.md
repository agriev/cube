# Grafana dashboards for the Cube Store HA fork

This directory ships ready-to-import Grafana dashboards for the
`cs.raft.*` metrics added in M9.1.

## Files

- [`raft-cluster.json`](raft-cluster.json) — operator dashboard
  for a 3-router HA cluster. Single-stat for current leader,
  time-series for term / commit-vs-applied / apply latency,
  rate panels for leader changes and proposes.

## Importing

In Grafana 10+:

1. **Dashboards → New → Import**.
2. Upload `raft-cluster.json` or paste its contents.
3. Pick your Prometheus datasource when prompted.
4. Save.

The dashboard expects metrics scraped from cubestore's
`/metrics` endpoint with `job="cubestore"`. Adjust the `job`
label in the panel queries if your scrape config uses a
different label.

## Metric names assumed

The dashboard queries assume the cubestore metric names land in
Prometheus with dots translated to underscores (the default
behavior of every Prometheus exporter we know of):

| In code | In Prometheus |
|---|---|
| `cs.raft.term` | `cs_raft_term` |
| `cs.raft.leader_id` | `cs_raft_leader_id` |
| `cs.raft.is_leader` | `cs_raft_is_leader` |
| `cs.raft.commit_index` | `cs_raft_commit_index` |
| `cs.raft.applied_index` | `cs_raft_applied_index` |
| `cs.raft.leader_changes` | `cs_raft_leader_changes` |
| `cs.raft.proposals.success` | `cs_raft_proposals_success` |
| `cs.raft.proposals.failed` | `cs_raft_proposals_failed` |
| `cs.raft.apply.duration_ms` | `cs_raft_apply_duration_ms` |

If your metrics pipeline uses a different naming convention
(StatsD-style dots, Datadog dots, etc.) the JSON is small enough
to find-and-replace.

## Alert ideas (not bundled)

Reasonable starting alerts that operators typically want:

- `rate(cs_raft_leader_changes[5m]) > 0.1` for >2m → leadership
  flapping.
- `cs_raft_term` increasing for >30s without a corresponding
  leader change → election storm.
- `cs_raft_commit_index - cs_raft_applied_index > 100` → apply
  path stuck.
- `histogram_quantile(0.99, ... apply_duration_ms ...) > 200ms`
  → write tail latency degraded.

These aren't shipped as alert rules because thresholds depend on
the deployment's QPS — set them when you have a baseline.
