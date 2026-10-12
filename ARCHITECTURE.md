# Architecture Specification: NexusFlow API

**System**: `nexusflow-api`  
**Version**: 1.0.0  
**Author**: Jash Patel (@jashp144)  
**License**: Apache License 2.0  

---

## 1. System Topology & Design Principles

`nexusflow-api` is designed around core principles of **event-driven resilience**, **mathematical scheduling fairness**, and **cryptographic integrity**:

- **Cohesion & Single Responsibility**: Task scheduling, dead-letter storage, cryptographic signing, circuit breaking, and HTTP transport layers are completely decoupled into dedicated modules.
- **Starvation-Free Guarantees**: Priority systems inherently risk starving low-priority workloads during high arrival bursts of critical tasks. NexusFlow integrates an explicit age-window mathematical promotion function.
- **Fail-Safe Downstream Protection**: Third-party webhook endpoints are inherently unreliable. NexusFlow isolates downstream disruptions using Circuit Breakers and decorrelated jittered backoffs to prevent cascading latency exhaustion.
- **Zero Heavyweight Broker Dependencies**: Runs entirely within standard Python 3.10+ async runtimes without mandatory external Redis or RabbitMQ dependencies for standalone or containerized deployment.

```
+-----------------------------------------------------------------------------------+
|                                NexusFlow Microservice                             |
|                                                                                   |
|   +------------------------+   HTTP Ingest    +-------------------------------+   |
|   |  FastAPI Application   | <--------------> | Client Producers & Telemetry  |   |
|   +------------------------+                  +-------------------------------+   |
|                |                                                                  |
|                v                                                                  |
|   +------------------------+                  +-------------------------------+   |
|   |   PriorityTaskQueue    |                  |       WebhookDispatcher       |   |
|   |  (Starvation-Free)     |                  |  - Circuit Breakers (Per-Sub) |   |
|   +------------------------+                  |  - HMAC-SHA256 Signature      |   |
|                |                              |  - Exponential Backoff        |   |
|                v                              +-------------------------------+   |
|   +------------------------+                                  |                   |
|   |   Async Worker Pool    |                                  v                   |
|   |  - Execution Loop      |                  +-------------------------------+   |
|   |  - Retry Supervisor    |                  |  External Webhook Endpoints   |   |
|   +------------------------+                  +-------------------------------+   |
|                |                                              |                   |
|                +--------------------+-------------------------+                   |
|                                     | (on failure exhaustion)                     |
|                                     v                                             |
|                      +-----------------------------+                              |
|                      |    DeadLetterQueue (DLQ)    |                              |
|                      |  - Poison Task Quarantine   |                              |
|                      |  - Diagnostic Audit & Replay|                              |
|                      +-----------------------------+                              |
+-----------------------------------------------------------------------------------+
```

---

## 2. Core Subsystems

### 2.1 Priority Task Queue & Starvation-Free Aging
Standard priority queues store elements ordered strictly by priority key $P$. Under sustained high arrival rates of $P_1$ tasks, tasks enqueued at $P_4$ experience infinite latency.

`PriorityTaskQueue` solves this dynamically:
- Priority ranks: `CRITICAL = 1`, `HIGH = 2`, `MEDIUM = 3`, `LOW = 4`.
- Dynamic effective priority calculation:
  $$\text{EffectivePriority}(t) = \max\left(1.0, \; \text{BasePriority} - \left\lfloor \frac{t_{\text{current}} - t_{\text{enqueued}}}{\Delta t_{\text{age\_window}}} \right\rfloor\right)$$
- When dequeuing, candidate selection evaluates the minimum effective priority tuple:
  $$\text{Candidate} = \arg\min_{i} \left(\text{EffectivePriority}_i, \; t_{\text{enqueued}, i}, \; \text{id}_i\right)$$
- **Theorem (Starvation Freedom)**: For any finite aging window $\Delta t > 0$ and maximum priority depth $K$, any waiting task enqueued at time $t_0$ with base priority $P_{\text{base}}$ reaches effective priority $1.0$ in at most $(P_{\text{base}} - 1) \cdot \Delta t$ seconds. Once at tier $1.0$, because its $t_{\text{enqueued}}$ is strictly strictly earlier than any newly arriving critical task, it is guaranteed to be dequeued before subsequent arrivals.

### 2.2 Dead Letter Queue (DLQ)
- Quarantines tasks exceeding `max_retries` or suffering terminal deserialization exceptions.
- Implements bounded storage with FIFO eviction of oldest poisoned tasks upon reaching `max_capacity`.
- Provides non-destructive `replay(task_id, queue)` allowing operators or automated self-healing scripts to re-enqueue rehabilitated tasks with reset retry counters.

### 2.3 Webhook Cryptographic Signatures & Anti-Replay
- Outbound requests sign payload bytes using shared secrets and current timestamps:
  $$\text{Digest} = \operatorname{HMAC-SHA256}\left(\text{secret}, \; t_{\text{epoch}} \mathbin{\Vert} \text{"."} \mathbin{\Vert} \text{Payload}_{\text{canonical}}\right)$$
- Payloads are normalized to canonical JSON (`separators=(',', ':')` with sorted keys).
- Verification enforces a strict sliding tolerance window $\tau_{\text{window}} = 300\text{s}$:
  $$|t_{\text{now}} - t_{\text{header}}| \le \tau_{\text{window}}$$
- Prevents attacker interception and re-transmission of historical valid payloads.

### 2.4 Downstream Circuit Breaker
- Downstream webhook endpoints are monitored via independent `CircuitBreaker` instances.
- State Machine:
  1. **CLOSED**: All requests permitted. Consecutive failures increment counter. If counter reaches `failure_threshold`, trips to **OPEN**.
  2. **OPEN**: All requests immediately rejected without network execution. A recovery timer runs for `recovery_timeout` seconds.
  3. **HALF_OPEN**: Permits trial probe requests. If probe requests succeed `success_threshold` consecutive times, state resets to **CLOSED**. Any failure immediately re-trips state to **OPEN**.

### 2.5 Exponential Backoff with Full Decorrelated Jitter
- Calculates retry sleep interval to prevent synchronized retry storms:
  $$T_{\text{cap}} = \min(T_{\text{max\_delay}}, \; T_{\text{base}} \times B^{\text{attempt}})$$
  $$T_{\text{sleep}} \sim \operatorname{Uniform}(0, \; T_{\text{cap}})$$

---

## 3. Concurrency & Thread-Safety Model

- Pure `asyncio` event loop architecture.
- Internal queue modifications are protected by `asyncio.Lock` and coordinate non-polling waiting using `asyncio.Event`.
- Worker tasks run as detached concurrent coroutines governed by FastAPI lifespan lifecycle management, guaranteeing clean drainage during SIGTERM/SIGINT interrupts.

---

## 4. Architectural Decision Records (ADRs)

### ADR-001: In-Process Asynchronous Priority Queue vs. External Broker
- **Context**: The microservice needs high scheduling throughput and dynamic mathematical aging without imposing multi-container infrastructure requirements during testing or edge deployment.
- **Decision**: Implement an in-process, lock-protected async priority queue with an extensible API interface.
- **Consequences**: Enables millisecond response times, zero network hop latency, and simplified single-binary packaging.

### ADR-002: Dynamic Aging Calculation vs. Periodic Ticking Thread
- **Context**: Updating the priority keys of waiting items in a priority heap normally requires $O(N \log N)$ periodic scanning.
- **Decision**: Evaluate effective priority dynamically upon `dequeue()` based on wall-clock timestamp difference.
- **Consequences**: Eliminates CPU wakeups during idle periods; ensures exact mathematical priority values upon selection without background thread jitter.

### ADR-003: HMAC-SHA256 Webhook Signatures with Canonical Formatting
- **Context**: Webhook recipients need cryptographic proof of authenticity and tamper-resistance.
- **Decision**: Standardize on SHA-256 HMAC over `{timestamp}.{canonical_json}` with constant-time verification.
- **Consequences**: Provides industry-standard security parity with Stripe and GitHub webhook delivery specifications.

### ADR-004: Circuit Breaker Isolation per Subscription URL
- **Context**: A failing third-party webhook destination should not degrade throughput or saturate connection pools for other subscribers.
- **Decision**: Isolate circuit breaker state machines per subscription ID.
- **Consequences**: Outages on partner endpoint A never throttle or delay deliveries to partner endpoint B.

### ADR-005: Full Jitter Exponential Backoff Strategy
- **Context**: When external services recover from an outage, synchronized retries from multiple deliveries can cause a secondary crash (thundering herd).
- **Decision**: Adopt AWS-recommended Full Jitter: $T_{\text{sleep}} = \operatorname{Uniform}(0, T_{\text{cap}})$.
- **Consequences**: Desynchronizes retry requests across distributed instances.

---

## 5. Failure Modes & Fault Isolation

| Failure Scenario | Mitigation | Resulting State |
|:---|:---|:---|
| Downstream Webhook 500 Error | Exponential backoff with jitter up to `max_attempts` | Retried; routed to DLQ if unrecovered |
| Downstream Endpoint Complete Outage | Circuit Breaker trips to `OPEN` after 5 consecutive failures | Early rejection without network latency |
| Poisoned Task Payload | Worker catches exception, increments `retry_count` | Routed to DLQ with detailed traceback |
| Queue Memory Saturation | Configurable `maxsize` rejects new tasks with `QueueFullError` | Returns HTTP 503 to upstream producer |
| Process Shutdown (SIGTERM) | FastAPI Lifespan cancels worker and awaits current task completion | Clean shutdown without orphaned tasks |

---

## 6. Security Architecture & Threat Model

1. **Replay Attacks**: Defended by strict timestamp validation against local system clock ($\pm 300\text{s}$ tolerance).
2. **Timing Attacks**: Signature comparisons use Python's `hmac.compare_digest` executing in constant time.
3. **Payload Tampering**: Any bitwise modification to serialized payload causes HMAC digest mismatch.
4. **Denial of Service**: Circuit breaker terminates repeated connections against degraded upstream hosts, preventing internal thread exhaustion.
