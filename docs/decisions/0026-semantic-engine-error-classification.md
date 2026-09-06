# ADR 0026: Classify engine failures by semantic outcome

- Status: accepted
- Date: 2026-09-06

## Decision

Keep the existing `BrokerError` variants and diagnostic text, and add a stable
engine-facing classification with two parts:

- `BrokerErrorKind` describes the semantic reason without requiring callers to
  know whether the backend used a file, JSON state, lock, or distributed
  transport.
- `BrokerErrorOutcome` describes the safe attempt boundary: `Rejected`,
  `Retryable`, or `Unknown`. A successful `Result` remains the confirmed
  outcome.

Callers that decide whether to retry must use `BrokerError::outcome()` rather
than matching `io::Error`, leader IDs, or free-form cluster messages. The
concrete error and its `Error::source()` remain available to logs and
operators. Generic storage, state, internal, corruption, and cluster failures
are conservatively `Unknown`; only failures for which the engine can prove
non-application are `Rejected` or `Retryable`.

## Rationale

The local and clustered engines already share the same `Result<T,
BrokerError>` boundary, but the enum mixed user-visible messaging failures
with backend representation and routing details. That forced every caller to
recreate retry-safety rules and made equivalent local and clustered outcomes
easy to classify differently. A small classification method fixes the
semantic boundary without a generic error framework or a provisional wire
change.

`NotLeader` is classified as `Retryable` because it is returned before the
current engine can apply the request. A generic `Cluster` failure remains
`Unknown`: the error text alone cannot prove whether a mutation crossed the
consensus or response boundary. The v1 server continues to emit the existing
error codes, so its conservative client mapping is unchanged until a
versioned protocol carries an explicit outcome class.

## Consequences

- Local and clustered implementations expose the same semantic classification
  for shared engine failures.
- Existing source users and wire clients retain the current variants, error
  strings, and provisional codes.
- Backend causes remain useful for diagnosis, but applications can avoid
  depending on storage or topology details.
- The classification is not operation-stage evidence. A future stage-aware
  protocol and the real-process ambiguity gates in the clustered outcome
  design remain necessary before automatic retries are exposed publicly.

## Evidence

The engine unit matrix covers every current `BrokerError` variant. Reusable
contract assertions run against both the local broker and the persistent
clustered engine, while existing real-server retry and cluster tests continue
to cover response loss, timeout, forwarding, and stale-delivery behavior.
The accepted v1 compatibility boundary is unchanged.
