# Design notes

Design notes turn evidence into unsettled architecture or implementation
proposals. They are useful for exploring concrete mechanisms and trade-offs,
including work that is not currently prioritized, but they do not authorize a
runtime, protocol, storage, or deployment change. Accepted behavior belongs in
the code, tests, current architecture, or a dated [ADR](../decisions/README.md).

## Evidence labels

Use these distinctions explicitly so a reader can tell what can be trusted as
current behavior and what is still a proposal:

- **Observed baseline** — behavior verified in code or tests at the dated
  revision named by the note. A later revision may invalidate it.
- **Sourced fact** — behavior or a mechanism documented by an external
  standard, paper, or product source. It is evidence for comparison, not a
  Runnel compatibility promise.
- **Inference or recommendation** — reasoning from observed and sourced facts;
  it remains open until an ADR accepts it.
- **Illustrative mechanism** — an example API, state shape, module boundary,
  or algorithm that makes an option concrete. It is not an implementation
  prescription unless an ADR says so.
- **Outcome/evidence gate** — the result and verification evidence a future
  implementation must establish. The implementation may use another design if
  it satisfies the same outcome and safety boundary.
- **Accepted constraint** — a boundary owned by current code or an ADR. Link
  the authoritative record instead of restating a competing contract here.

## Authoritative homes

- Current technical boundaries: [architecture](../architecture.md).
- Accepted rationale and compatibility consequences: [decisions](../decisions/README.md).
- Unfinished product outcomes: [backlog](../backlog.md).
- Verification and benchmark policy: [testing](../testing.md) and
  [benchmarking](../benchmarking.md).
- Current implementation behavior: Rust code and its tests.

Each substantial note should state its status, last-review date, and baseline
revision. Keep historical observations tied to that baseline, preserve source
links and alternatives, and qualify rankings with their assumptions and
missing evidence. Do not present a staged sequence as a task list unless the
note explicitly records an accepted implementation plan; otherwise describe
stages as outcome/evidence gates and keep API and module names illustrative.
