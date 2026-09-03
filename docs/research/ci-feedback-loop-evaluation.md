# Hosted CI/CD evaluation: CircleCI Cloud against GitHub Actions

- Status: desk evaluation complete; hosted trial not run
- Last reviewed: 2026-09-03
- Scope: compare CircleCI Cloud with the current GitHub Actions workflow using
  Runnel's pull-request DAG and scheduled benchmark workloads
- Primary evidence class: design/research
- Secondary tags: tooling/CI
- Baseline: `87db3b8d7c3bdb03b44778b795ca72e4efa2e0c2`

This is a source-backed evaluation, not a hosted performance result. It records
what can be established from the repository, observed GitHub runs, and current
official platform documentation. The CircleCI workflow was not installed or
executed.

## Conclusion

CircleCI Cloud is a credible trial candidate for Runnel. Its workflows and
`requires` dependencies can represent the existing independent pull-request
checks and the `benchmark -> publish` history DAG. Its managed Linux VM
executor has full Docker access, and its documented failed-job reruns, SSH
debugging, and resource views could improve the failure-recovery experience.

There is not enough evidence to call it materially better. The GitHub App
integration explicitly does not trigger pipelines for forked pull requests,
which is a direct coverage risk for the current `pull_request` workflows.
CircleCI also requires a second VCS/checks integration, a new configuration and
credential path for `benchmark-history`, explicit cache configuration, and a
different credit and retention model. None of those costs or behaviors were
measured against Runnel here.

Recommendation: retain GitHub Actions as the required CI/CD system and defer a
CircleCI migration. If access is approved, run a time-boxed shadow trial with
no changes to required checks. The backlog outcome remains open until that
trial produces equivalent-coverage measurements.

## Evidence boundary

The branch was checked against the required baseline before this evaluation:

- `git fetch origin main` returned `origin/main` at
  `87db3b8d7c3bdb03b44778b795ca72e4efa2e0c2`.
- The evaluation branch was clean and `HEAD` was exactly that revision.
- The latest `ci.yml` run for that SHA was completed successfully:
  [GitHub Actions run 33798263890](https://github.com/winrarr/runnel/actions/runs/33798263890).
- The repository is public. This matters to the platform cost comparison
  because both vendors publish different public/open-source allowances.

Repository evidence comes from `.github/workflows/ci.yml`,
`.github/workflows/security.yml`, `.github/workflows/benchmarks.yml`,
`.github/workflows/benchmark-competitors.yml`, and [the local testing
guide](../testing.md). Platform claims are based on the official
[GitHub-hosted runner documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job),
[GitHub dependency-cache documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching),
[GitHub artifact settings](https://docs.github.com/en/enterprise-cloud@latest/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository),
and CircleCI's [workflow orchestration](https://circleci.com/docs/guides/orchestrate/workflows/),
[GitHub integration](https://circleci.com/docs/guides/integration/enable-checks/),
[Linux VM executor](https://circleci.com/docs/guides/execution-managed/using-linuxvm/),
[caching](https://circleci.com/docs/guides/optimize/caching/),
[Docker layer caching](https://circleci.com/docs/guides/optimize/docker-layer-caching/),
[automatic reruns](https://circleci.com/docs/guides/orchestrate/automatic-reruns/),
[SSH debugging](https://circleci.com/docs/guides/execution-managed/ssh-access-jobs/),
[artifact storage](https://circleci.com/docs/guides/optimize/artifacts/),
[pricing](https://circleci.com/pricing/), and [the current price
list](https://circleci.com/pricing/price-list/).

## Current GitHub Actions topology

### Pull-request DAG

The representative pull-request boundary is three independent checks. The
first two are roots in `ci.yml`; the third is a root in a separate workflow.

```text
pull_request
  ├── CI / Verify
  │     └── ubuntu-24.04, 15-minute timeout, `just verify`
  ├── CI / Integration and container smoke tests
  │     └── ubuntu-24.04, 15-minute timeout, Buildx image build, `just integration`
  └── Security / Audit
        └── ubuntu-24.04, pinned `cargo-audit` lockfile audit
```

There are no `needs` edges between the two CI jobs, so verification and
integration run concurrently when runners are available. The CI workflow
uses a per-workflow/per-ref concurrency group with cancellation enabled. The
security workflow has no corresponding concurrency declaration. This means a
candidate must be checked both for independent job scheduling and for the
behavior of rapid pushes to one pull-request branch.

`just verify` covers formatting, Clippy, workspace tests including the real
process cluster smoke test, documentation tests, ShellCheck, benchmark-script
tests, and a workspace build. `just integration` covers the isolated process
smoke test, Docker image setup, single-node container smoke, and three-node
container smoke. Integration captures runner diagnostics and uploads them on
failure; the current verify and security jobs do not upload equivalent runner
snapshots.

### Scheduled and manual DAGs that must remain covered

These are not pull-request latency gates, but they are part of the evaluation
boundary because a CI/CD replacement must not silently lose them.

| Workflow | Trigger | DAG and workload | Outputs and operational behavior |
| --- | --- | --- | --- |
| `benchmarks.yml` | Daily schedule and manual dispatch | `benchmark`: three repetitions of single-node Runnel comparisons and three-node Runnel runs. Defaults are 10,000 single-node messages, 200 clustered messages, payloads 100/1024 bytes, 2 CPUs, 2 GB broker memory, cluster warmup 100, and concurrency 2. | Uploads raw/normalized/aggregate results, then `publish` downloads them and appends the generated history to `benchmark-history`. The `benchmark-history` concurrency group does not cancel in-progress runs. |
| `benchmark-competitors.yml` | Weekly schedule and manual dispatch | `benchmark`: three repetitions of native Kafka/Redpanda/NATS comparisons and three-node replicated comparisons. Defaults are 10,000 messages, payloads 100/1024 bytes, 2 CPUs and 2 GB broker memory, plus 1 CPU and 512 MB client limits. | Uploads raw/normalized/aggregate results, then `publish` appends the separate competitor history to `benchmark-history`. It uses the same non-cancelling history concurrency group. |
| `security.yml` | Pull requests, weekly schedule, and manual dispatch | Pinned `cargo-audit` plus a guard that fails if the currently ignored advisory becomes active in the feature graph. | No artifact is produced on success; the audit log is the primary output. |

The benchmark jobs have a 40-minute timeout and expose message, clustered
message, and repetition inputs. The candidate must preserve those manual
controls, the sequential repetition semantics, the artifact handoff, and the
write to `benchmark-history`. A successful run of only `just verify` is not
equivalent coverage.

## GitHub baseline measurements available here

The following are observations from recent successful `main` push runs, not a
controlled benchmark. Times are calculated from the run and job timestamps
reported by GitHub. Workflow elapsed time includes time waiting for a runner;
job duration starts when the job begins executing.

| Run | Workflow elapsed | Verify job | Integration job | Observation |
| --- | ---: | ---: | ---: | --- |
| [33798263890](https://github.com/winrarr/runnel/actions/runs/33798263890), baseline SHA | 3m51s | 3m11s | 3m47s | Integration image build was 1m44s; the `just integration` step was 1m37s. |
| [33789947838](https://github.com/winrarr/runnel/actions/runs/33789947838) | 5m15s | 3m26s | 3m51s | Both jobs waited about 81 seconds before starting. |
| [33786203382](https://github.com/winrarr/runnel/actions/runs/33786203382) | 4m54s | 2m37s | 3m58s | Both jobs waited about 55 seconds before starting. |
| [33785348288](https://github.com/winrarr/runnel/actions/runs/33785348288) | 5m05s | 4m03s | 3m53s | Verify waited about 61 seconds; integration waited about 72 seconds. |
| [33769610401](https://github.com/winrarr/runnel/actions/runs/33769610401) | 13m59s | 3m04s | 2m12s | Verify did not start until about 10m54s after workflow creation; queue time dominated the wall clock. |

The baseline run is therefore roughly four minutes of runner execution on a
successful warm-ish path, with observed workflow wall time from 3m51s to
13m59s in this small sample. The sample spans different commits, runner
availability, and unrecorded cache state, so it does not establish a GitHub
performance distribution or a provider advantage. It does establish that a
trial must separate queue time, executor startup, cache restore, image build,
test execution, and post-job upload time. No comparable CircleCI timestamps,
cache hits, retries, artifacts, or contributor interactions are available.

The latest baseline run was a `push` run, so it does not include the separate
pull-request security job. A security timing comparison needs an actual PR
event or an equivalent manual trial invocation.

## CircleCI equivalence assessment

The proposed candidate is CircleCI Cloud using the GitHub App integration and
managed x86 Linux VMs. It would use an explicit `ubuntu-2404:current` machine
image and, if the account permits it, `large.gen2` (4 vCPUs, 16 GiB) to match
the current GitHub standard runner's documented 4 vCPU/16 GB profile. CircleCI
provides 150 GB VM storage, while the current GitHub standard runner documents
14 GB SSD storage; disk pressure must therefore be measured explicitly rather
than inferred from CPU/RAM matching.

| Capability | CircleCI mapping | Equivalence risk or expected difference |
| --- | --- | --- |
| PR triggers | GitHub App triggers support pull-request opened, synchronize, reopened, ready-for-review, and related events. | The official trigger documentation says forked pull requests never trigger GitHub App pipelines. The current GitHub `pull_request` trigger is therefore not equivalent until fork behavior and a safe alternative integration are validated. |
| PR DAG | Define `verify`, `integration`, and `security_audit` as root jobs in a workflow, with no `requires` edges. CircleCI jobs run concurrently unless dependencies are declared. | Job checks may be grouped or named differently in GitHub. Keep the trial non-required and record exact check names before considering branch protection changes. |
| Benchmark DAG | Define one scheduled/manual workflow per history suite with a `benchmark` job and a `publish` job requiring it. CircleCI supports scheduled workflows and workflow dependencies. | `publish` must authenticate a write to `benchmark-history`; this is a new secret/permission path and must never be available to untrusted fork jobs. |
| Rust toolchain and commands | Use the same pinned Rust toolchain, `just`, and repository commands in VM steps. | GitHub Actions currently supplies toolchain/install actions. CircleCI requires explicit installation or a pinned image step, adding configuration and maintenance surface. |
| Docker integration | Use the CircleCI machine executor, which provides full Docker access and supports Ubuntu 24.04 images and Docker Layer Caching. | Docker Layer Caching is not the same as the current Buildx `type=gha` cache. Named Buildx builders are required for DLC reuse, and CircleCI documents that a cache saved at job teardown is generally unavailable to another job in the same workflow. |
| Rust/Cargo cache | Configure explicit `restore_cache`/`save_cache` paths and keys for Cargo registry, git, and build artifacts as appropriate. | CircleCI caches are immutable, manually configured, and retained for at most 15 days. A key race can make the first writer win. The trial must record exact and partial hits and avoid sharing a write key across concurrent root jobs. |
| Artifacts | Use `store_artifacts` for benchmark results and failure diagnostics; use a workspace only for the benchmark-to-publish handoff. | CircleCI artifact retention defaults to and is capped at 30 days, with a 3 GB maximum per file. This is shorter than GitHub's public repository artifact/log maximum of 90 days unless the existing GitHub setting is lower. The history commit itself remains the durable record. |
| Status checks | Enable CircleCI GitHub Checks for workflow-level status and/or default job-level status updates. | Enabling Checks requires GitHub repository administration and produces CircleCI-specific check names. GitHub's Checks UI rerun control reruns all checks rather than offering granular selection; CircleCI's own UI can rerun failed jobs. Existing required checks must remain GitHub Actions during the trial. |
| Retries | CircleCI supports manual rerun-from-failed and configurable step/workflow automatic reruns, capped at five. | Automatic retries consume credits and can hide flaky tests. The first trial should leave automatic retries off, use controlled failure probes, and report manual reruns separately from first-attempt success. `setup_remote_docker` and checkout are not eligible for automatic step reruns. |
| Diagnostics | CircleCI provides real-time job logs, a resource view, and rerun-with-SSH access to the job VM. | This could improve on the current integration-only failure snapshot, but it adds an external UI and requires an SSH key. It does not automatically diagnose a nondeterministic Rust or Docker failure. |
| Contributor ergonomics | Contributors continue to open GitHub pull requests and see statuses in GitHub; CircleCI provides a linked pipeline view. | Contributors and maintainers must learn a second UI, CircleCI config syntax, check naming, and cache controls. Fork PR behavior is the largest unresolved ergonomics and coverage issue. |
| Cancellation and queues | CircleCI can auto-cancel redundant workflows on non-default branches and has serial groups for organization-wide serialization. | GitHub's current CI concurrency group cancels same-workflow runs on the same ref, while benchmark history explicitly does not cancel. CircleCI's auto-cancel feature excludes the default branch and scheduled/rerun workflows, so the exact policy must be configured and tested per workflow. |

CircleCI can express the topology, but configuration expressibility is not the
same as equivalent behavior. The fork trigger, Docker cache, write credential,
status names, cancellation policy, and resource class all require direct trial
evidence.

## Cold, warm, and variability plan

If a hosted trial is authorized, register the following before the first run.
Do not compare a GitHub warm cache with a CircleCI cold cache or compare
different resource classes.

1. Use one fixed repository revision for paired runs and the same pinned
   toolchain, base image, message counts, payload sizes, repetition counts,
   timeout values, and Docker inner-container CPU/memory limits.
2. Run the PR DAG with at least 10 cold and 10 warm repetitions per provider.
   Cold means a newly selected cache key or explicitly purged provider cache,
   a fresh executor, and a first Docker image build. Warm means the same
   revision and lockfiles are repeated with documented cache keys and the
   provider reports an exact cache hit. A cache miss is not a failure, but it
   must be counted.
3. Run each scheduled benchmark workflow at least once cold and once warm with
   its normal defaults and its internal `repetitions=3`. This verifies the
   benchmark workload, artifact handoff, and history publication without
   turning the expensive history suite into a PR gate. Repeat if a provider
   fails or the cache state is ambiguous.
4. Exercise security on a real pull-request event, including a fork PR if the
   candidate integration claims to support it. Use a disposable branch for a
   controlled failure and verify logs, failed-job rerun, artifact retrieval,
   and check completion. Do not give write credentials to that failure probe.
5. Run providers in alternating order and keep the provider runs sequential
   when comparing a pair. Record the commit, pipeline/run ID, queue start,
   executor start, every job start/finish, cache key and hit class, Docker
   build duration, command duration, artifact upload/download duration, retry
   count, cancellation, failure classification, resource class, CPU/RAM/disk,
   credits, and storage use.

Report at least p50, p95, minimum, maximum, and sample count for time to first
check and time to all required checks. Report queue, executor startup, cache,
build, test, and upload components separately. Treat a failure that succeeds
only after retry as a first-attempt failure, and report retry cost and time
separately. The suggested materiality rule is no coverage regression and at
least a 20% lower median time to all checks with no worse p95; a diagnostics
improvement alone may justify a limited pilot but not an unmeasured migration.
The rule must be accepted before collecting results.

## Cost and usage constraints

GitHub documents standard hosted runner use as free and unlimited for public
repositories. Its current documentation also describes a default 10 GB cache
limit per repository, eviction after more than seven days without access, and
public artifact/log retention configurable from one to 90 days. The repository
does not record its current storage settings in the workflow, so those limits
should be sampled from repository settings during a future trial.

CircleCI's current pricing documentation lists up to 400,000 free credits per
month for Linux open-source builds, 30,000 credits per month for non-open-source
Free-plan use, and 30x concurrency on the Free plan. It also documents 25,000
credits for $15 on the Performance plan, though account eligibility and
plan-specific resource access still need confirmation. The current price list
puts a 4 vCPU/16 GiB Gen2 Linux VM at 36 credits/minute and Docker Layer
Caching at 200 credits per job.

Using the observed roughly four-minute GitHub CI path only as a sizing example,
two equivalent four-minute `large.gen2` CircleCI jobs would consume about 288
compute credits, plus 200 credits if the integration job enables DLC, before
the security job, retries, storage, and scheduled benchmarks. A benchmark job
that runs for its full 40-minute timeout at that resource class would consume
up to 1,440 compute credits, again before DLC and storage. These are planning
calculations, not measurements or a cost quote.

CircleCI documents 15-day maximum retention for caches and workspaces and
30-day maximum retention for artifacts. It also documents that queued or
preparing jobs are not charged, so queue time must still be measured for
developer experience even though it is not compute spend. Automatic reruns and
DLC add usage. The benchmark workload's daily and weekly schedule makes a
credit ledger necessary even if the public open-source allowance is sufficient.

## Shortcomings addressed and not addressed

Potentially addressed by CircleCI, subject to trial evidence:

- failed-job rerun and automatic retry controls are more explicit than the
  current workflows' manual GitHub rerun path;
- SSH rerun and resource views could reduce time spent reconstructing a failed
  integration runner;
- explicit workflow-level DAG visualization and job-level status can make the
  independent checks easier to inspect;
- machine-executor Docker access is a direct fit for `just integration` and the
  containerized benchmark harness.

Not addressed by changing platforms alone:

- Rust compilation, Docker image build, real-process integration, or benchmark
  work itself remains the same workload;
- queue variability and shared hosted-runner contention are not guaranteed to
  improve;
- flaky tests still need root-cause work, and automatic retries can hide them;
- fork PR coverage, GitHub check naming, write credentials for history, and
  cache semantics introduce new risks;
- CircleCI does not preserve GitHub Actions' native action ecosystem or remove
  the need to maintain pinned toolchains and repository scripts.

## Setup, maintenance, and rollback

A shadow implementation would add a `.circleci/config.yml` with reusable
commands for checkout/toolchain setup, `just verify`, `just integration`, the
security audit, the two benchmark jobs, artifact storage, and the history
publisher. It would also require a CircleCI GitHub App installation, project
settings, a safe GitHub token or deploy key for the history publisher, and a
policy for which contexts/secrets are available to pull requests. This is a
meaningful second configuration surface even if all domain work remains in
the existing scripts.

The safe rollback is to leave all GitHub Actions workflows and required checks
enabled throughout the trial, disable the CircleCI project/check integration,
and remove the trial configuration from its disposable branch or project. No
Runnel data or source-control history needs migration. If a future migration
were accepted, GitHub Actions should remain enabled until CircleCI check names,
fork behavior, benchmark publication, and the rollback commit are verified;
then reverting the CircleCI configuration and restoring the GitHub branch-rule
checks returns the repository to the current state.

## Blockers and evidence gaps

No hosted CircleCI execution was possible in this environment. The blocker is
not a code failure: there is no authorized CircleCI organization/project,
CircleCI account or billing context, GitHub App installation permission, or
approved write credential for the `benchmark-history` publisher. This task's
scope also excludes adding a trial workflow or changing required checks.

Consequently, the following remain unknown:

- CircleCI queue and wall-clock distributions for Runnel's exact jobs;
- cold versus warm Cargo and Docker cache hit rates and restore/build times;
- behavior of the three-check PR status surface, especially fork PRs;
- artifact and benchmark-history publication under least-privilege credentials;
- failure diagnostics, SSH access, retry behavior, and contributor task times;
- actual credits, storage, and any plan-specific resource/concurrency limits for
  this repository.

The GitHub observations above are sufficient to register a baseline and a
measurement method, not to accept the backlog outcome.

## References

All platform references were checked on 2026-09-03:

- [GitHub-hosted runner selection and standard runner sizes](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job)
- [GitHub dependency cache behavior, limits, and eviction](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)
- [GitHub artifact and log retention settings](https://docs.github.com/en/enterprise-cloud@latest/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository)
- [GitHub Actions concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)
- [CircleCI workflow DAGs, schedules, workspaces, and failed-job reruns](https://circleci.com/docs/guides/orchestrate/workflows/)
- [CircleCI GitHub Checks and status reporting](https://circleci.com/docs/guides/integration/enable-checks/)
- [CircleCI GitHub trigger events and forked pull-request limitation](https://circleci.com/docs/guides/orchestrate/github-trigger-event-options/)
- [CircleCI managed Linux VM executor](https://circleci.com/docs/guides/execution-managed/using-linuxvm/)
- [CircleCI resource classes](https://circleci.com/docs/reference/configuration-reference/)
- [CircleCI dependency caching and retention](https://circleci.com/docs/guides/optimize/caching/)
- [CircleCI Docker Layer Caching](https://circleci.com/docs/guides/optimize/docker-layer-caching/)
- [CircleCI automatic reruns](https://circleci.com/docs/guides/orchestrate/automatic-reruns/)
- [CircleCI SSH rerun debugging](https://circleci.com/docs/guides/execution-managed/ssh-access-jobs/)
- [CircleCI artifact storage](https://circleci.com/docs/guides/optimize/artifacts/)
- [CircleCI pricing and plans](https://circleci.com/pricing/)
- [CircleCI current credit price list](https://circleci.com/pricing/price-list/)
