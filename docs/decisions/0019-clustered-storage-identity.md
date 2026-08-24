# ADR 0019: Fail closed on mismatched clustered storage identity

- Status: accepted
- Date: 2026-08-24

## Decision

Clustered durable state records its storage format version, cluster identity,
and node identity in an explicit metadata file. A broker may open the
directory only when those values match its configured identity and the
supported format version.

An existing clustered directory that contains state but no identity metadata
is rejected instead of being guessed or silently adopted. New empty
directories may initialize their metadata during startup. The initial
implementation does not migrate unmarked state automatically.

## Rationale

A node identity is part of the safety boundary for replicated state. Opening a
directory under a different cluster or node identity can make an old Raft log,
snapshot, or state machine appear valid while violating membership and fencing
assumptions. Failing closed makes the ambiguity visible before the process
serves traffic or participates in consensus.

## Consequences

- Accidental reuse of persistent clustered storage is detected at startup.
- Existing early clustered directories without metadata require an explicit
  operator migration or recreation before they can be used by this version.
- The metadata file is a local compatibility marker, not a public protocol
  concept.
- Future replacement and migration workflows must preserve this identity
  boundary while defining explicit adoption, rejoin, and fencing semantics.

## Verification

The clustered storage tests cover matching identity, cluster and node
mismatches, invalid metadata, unsupported versions, legacy unmarked state, and
new-directory initialization.
