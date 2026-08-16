# kafka-databricks-pipeline

                    ┌──────────────────┐
                    │  Clickstream App │
                    │   / Web / Mobile │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Kafka Producer   │
                    │ user_id as key   │
                    └────────┬─────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │ Kafka: clickstream      │
                │                         │
                │ 12 partitions           │
                │ P0 P1 P2 ... P11        │
                └────────────┬────────────┘
                             │
                    Consumer Group
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
          Consumer-1     Consumer-2     Consumer-3
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                    Validation / Retry
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
                Valid events       Invalid
                    │                 │
                    ▼                 ▼
             Processing          Kafka DLQ
                    │
                    ▼
              ┌──────────────┐
              │ Databricks  │
              │             │
              │ Bronze      │
              │ Delta Table │
              └──────┬───────┘
                     ▼
              Silver / Gold
                     │
                     ▼
                Analytics


We'll implement:
12 partitions
user_id as Kafka key
Multiple consumer instances
Same consumer group
Batch polling
Manual offset management
Pydantic validation
Bounded retries
Exponential backoff
Dead Letter Queue
Structured JSON logging
Graceful shutdown
Idempotency awareness
Partition/offset tracking


