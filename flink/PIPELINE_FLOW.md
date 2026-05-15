# Flink Pipeline Flow

## Failure Handling — Current Status

Inventory of every failure mode in the pipeline today, the gap vs. a sustainable design, and the target behavior. Each failure is classified into one of three classes:

| Class                  | Mechanism                                                       |
|------------------------|-----------------------------------------------------------------|
| Permanent (bad data)   | DLQ — replay can't fix the payload                              |
| Transient (infra blip) | In-operator retry with bounded backoff → re-throw if exhausted  |
| Infra misconfig        | `log.error(...)` + re-throw immediately                         |

### Parse stage (`ParseEventFunction`)

| # | Failure                                           | Class     | Current behavior | Gap              | New behavior                                            |
|---|---------------------------------------------------|-----------|------------------|------------------|---------------------------------------------------------|
| 1 | Bad JSON / missing required field / bad timestamp | Permanent | DLQ              | -                | -                                                       |
| 2 | Unknown `eventType` (not DATA/IMAGE)              | Permanent | Silently dropped | No observability | Drop + emit counter metric `flink.events.unknown_type`  |

### MinIO upload stage (`MinioUploadFunction`)

| # | Failure                                                     | Class           | Current behavior | Gap                                            | New behavior                                                                               |
|---|-------------------------------------------------------------|-----------------|------------------|------------------------------------------------|--------------------------------------------------------------------------------------------|
| 3 | Bad base64 / bad URL / disallowed scheme or host / HTTP 4xx / response > 10 MB | Permanent       | DLQ              | -                                              | -                                                                                          |
| 4 | HTTP 5xx, HTTP timeout (connect > 10 s or read > 30 s)      | Transient       | In-operator retry (3 attempts, exponential backoff: 1 s / 2 s) → DLQ if exhausted | -   | -                                                                                          |
| 5 | MinIO auth / bucket missing / disk full                     | Infra misconfig | DLQ              | Silent data routing instead of loud failure    | `log.error(...)` + re-throw immediately                                                    |

### Postgres write stage (`PostgresProcessedEventWriter` / `PostgresEventTypeCount5mWriter`)

| # | Failure                                         | Class           | Current behavior | Gap                       | New behavior                                                                               |
|---|-------------------------------------------------|-----------------|------------------|---------------------------|--------------------------------------------------------------------------------------------|
| 6 | Constraint violation / invalid JSONB            | Permanent       | Swallowed        | Data loss                 | DLQ                                                                                        |
| 7 | Connection timeout / refused / deadlock         | Transient       | Swallowed        | Data loss; no retry       | In-operator retry with bounded backoff (e.g. 3 attempts: 1s/2s/4s) → re-throw if exhausted |
| 8 | Auth failure / schema mismatch (missing column) | Infra misconfig | Swallowed        | Data loss; silent failure | `log.error(...)` + re-throw immediately                                                    |

### Cross-cutting

| #  | Failure                                         | Class     | Current behavior      | Gap | New behavior |
|----|-------------------------------------------------|-----------|-----------------------|-----|--------------|
| 9  | DLQ Kafka sink fails / checkpoint storage fails | Infra     | Job dies              | -   | -            |
| 10 | TaskManager crash                               | Transient | Restart from checkpoint | - | -            |

## Sequence Diagram

```mermaid
sequenceDiagram
    participant K as Kafka (events)
    participant PEF as ParseEventFunction
    participant SJ as StreamingJob (filter/route)
    participant MU as MinioUploadFunction
    participant MINIO as MinIO
    participant PG as PostgreSQL
    participant KDLQ as Kafka (events-dlq)

    K->>PEF: raw JSON string

    alt parse fails
        PEF->>SJ: DlqRecord (side output tag)
        SJ->>KDLQ: KafkaSink produces DlqRecord
    else parse succeeds + IMAGE
        PEF->>SJ: ProcessedEvent (main output)
        SJ->>MU: ProcessedEvent
        MU->>MINIO: PUT images/{date}/{uuid}.ext
        MU->>PG: INSERT processed_events
    else parse succeeds + DATA
        PEF->>SJ: ProcessedEvent (main output)
        SJ->>PG: INSERT processed_events
    end
```
