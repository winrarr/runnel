# ADR 0025: Retain GitHub Actions pending a hosted CI trial

- Status: accepted
- Date: 2026-09-03

## Decision

Keep GitHub Actions as Runnel's required CI/CD system. Do not migrate required
checks or scheduled benchmark publication to CircleCI based on desk research.
CircleCI Cloud remains a candidate for a bounded, shadow-only trial if the
repository owner can provide an authorized account, GitHub App integration, and
least-privilege benchmark publication credentials.

The trial must preserve the current pull-request checks, security audit,
scheduled/manual Runnel benchmark history, competitor benchmark history,
artifacts, status reporting, and rollback path. It must run outside required
checks until equivalent behavior is demonstrated.

## Rationale

[The hosted CI evaluation](../research/ci-feedback-loop-evaluation.md) finds
that CircleCI can express the current independent PR DAG and the benchmark
`benchmark -> publish` DAG. Its managed Linux VM and Docker Layer Caching fit
the repository's process and container workloads. Failed-job reruns, SSH
debugging, and resource views may improve failure diagnosis.

However, no CircleCI run was available. The GitHub App integration explicitly
does not trigger forked pull requests, and the candidate's cache, Docker,
status-check, cancellation, credential, credit, and artifact-retention
behavior differs from GitHub Actions. The current GitHub sample also shows
meaningful queue variability, but it is not controlled evidence that CircleCI
would reduce it.

The required evidence for a switch is therefore an actual equivalent-coverage
trial, not a configuration translation or a vendor feature comparison.

## Trial gate

Before a future trial, pre-register the exact revisions, resource classes,
workloads, cache states, repetitions, and measurements. Use at least 10 cold
and 10 warm paired PR runs per provider, plus cold and warm executions of each
scheduled benchmark workflow with its normal internal repetitions. Include a
real security pull-request event and test a fork PR if the candidate claims to
cover it. Record queue time separately from job time, count retries as
first-attempt failures, and account for credits, artifacts, and storage.

The suggested materiality rule is zero coverage regression and at least a 20%
lower median time to all checks with no worse p95. A diagnostics-only benefit
can justify a limited pilot, but not an unmeasured migration.

## Consequences and rollback

- GitHub remains the single required status surface and source-control system.
- The development feedback loop receives a measured baseline and a documented
  candidate, but no immediate latency or reliability improvement is claimed.
- A future trial temporarily adds configuration and maintenance work; it must
  not change required checks or expose write credentials to untrusted PRs.
- Rollback is to disable the CircleCI project/check integration and remove the
  trial configuration, leaving GitHub Actions and `benchmark-history` intact.

The backlog outcome [Improve the development feedback loop](../backlog.md)
remains open because the hosted trial and its evidence package do not yet
exist.
