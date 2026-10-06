# TD-028: Historical RNL1 resource-policy proposal

- Status: superseded by [ADR 0039](../decisions/0039-rnl1-write-admission-and-legacy-read-compatibility.md)
- Scope: historical rationale for the former compatibility policy
- Current debt: [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget)

The earlier proposal capped new RNL1 writes while preserving all complete historical RNL1 reads. It avoided making a read-ceiling decision without a representative persisted-data corpus, and therefore proposed a separate inspection/export route before any old record could become unreadable.

That compatibility premise does not apply: Runnel has no deployed users or backward-compatibility requirement. ADR 0039 replaces the multi-format reader and writer selectors with one RNL3 v2 local format. RNL1, RNL2, and RNL3 v1 stream files now fail clearly before incomplete-tail repair; no maintained audit or export utility is part of recovery.

The useful resource finding remains narrower: per-record field bounds do not establish an aggregate process-memory budget. Recovery indexes, retained request identities, response encoding, and concurrent payload materialization remain open in TD-028.
