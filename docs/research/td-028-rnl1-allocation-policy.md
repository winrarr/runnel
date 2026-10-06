# TD-028: Historical RNL1 allocation-policy evidence

- Status: historical evidence; the proposed legacy-read policy is superseded by [ADR 0039](../decisions/0039-rnl1-write-admission-and-legacy-read-compatibility.md)
- Current debt: [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget)

The earlier investigation found that RNL1's u32 key and payload lengths permitted records much larger than the later per-field limits. It rejected blindly applying those limits to complete RNL1 records because no representative store corpus established whether data would become unreadable. It proposed read-only inventory and export as prerequisites for any future read ceiling.

The compatibility constraint has been removed: Runnel has no deployed users, and ADR 0039 selects checksummed RNL3 v2 for every local record while refusing RNL1, RNL2, and RNL3 v1. The historical inspector/export proposal is obsolete; there is no maintained RNL1 audit path.

One conclusion remains useful for the current format: a fetch or message-size limit does not by itself bound aggregate broker memory. The former comparison used Kafka's distinct [broker record-batch limit](https://kafka.apache.org/42/configuration/broker-configs/#brokerconfigs_message.max.bytes) and [consumer fetch budget](https://kafka.apache.org/42/configuration/consumer-configs/#consumerconfigs_fetch.max.bytes) as examples of separate controls. Those systems do not prescribe Runnel's policy. TD-028 now tracks aggregate indexes, response copies, and concurrent current-format reads.
