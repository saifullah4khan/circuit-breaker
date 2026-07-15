# circuit-breaker

Stop hammering a dependency that is already down. A small, dependency-free
circuit breaker for Python that trips open after repeated failures, rejects
calls fast during the outage, then quietly probes for recovery.

## The problem

When a downstream service (a payment API, an LLM provider, a database) starts
failing, the worst thing your app can do is keep calling it. Every doomed
request ties up a worker, stacks up timeouts, and slows the failure down for
your own users. Retries make it worse: now you are hitting a struggling service
harder, right when it needs room to recover.

A circuit breaker is the standard fix. After a run of failures it "opens" and
fails calls instantly for a short cooldown instead of waiting on a service that
will not answer. Once the cooldown passes it lets a few trial calls through, and
only returns to normal when those succeed. This library implements that state
machine in about 200 lines with no third-party dependencies, a clock you can
fake in tests, and enough knobs to fit real workloads.

## Install

```bash
pip install circuit-breaker-lite
```

Or drop the single `circuit_breaker/` package into your project. It only uses
the standard library.

## Quickstart

```python
from circuit_breaker import CircuitBreaker, CircuitOpenError

breaker = CircuitBreaker(
    failure_threshold=5,     # trip after 5 straight failures
    recovery_timeout=30.0,   # stay open for 30s before probing
    name="payments-api",
)

try:
    result = breaker.call(charge_customer, order_id, amount)
except CircuitOpenError as e:
    # The circuit is open: skip the call and degrade gracefully.
    # e.retry_after tells you how long until the next probe.
    queue_for_later(order_id)
```

Prefer a decorator?

```python
@breaker
def charge_customer(order_id, amount):
    ...
```

Or guard a block:

```python
with breaker:
    charge_customer(order_id, amount)
```

All three share the same breaker state, so you can mix them across a codebase
that talks to the same dependency.

## How it behaves

The breaker is a three-state machine:

- **closed** - calls pass through. Consecutive failures are counted; a success
  resets the count. Hit `failure_threshold` and it trips to open.
- **open** - calls are rejected immediately with `CircuitOpenError` and the
  protected function is never invoked. After `recovery_timeout` seconds it moves
  to half-open.
- **half-open** - up to `half_open_max_calls` trial calls are allowed through.
  Reach `success_threshold` successes and it closes. A single failure sends it
  straight back to open and restarts the cooldown.

## Design decisions

**Why count consecutive failures, not a percentage.** A rolling error-rate
window is more precise but needs bookkeeping and a minimum-volume rule to avoid
tripping on the first request of the day. Consecutive failures are simple,
predictable, and match how transient outages actually look: a burst of
back-to-back errors. A single success in the closed state clears the streak, so
a lone blip never counts against you.

**Why `expected_exception` defaults to `Exception` and can be narrowed.** Not
every exception means the dependency is down. A `ValueError` from your own
argument handling is a bug, not an outage, and tripping the breaker on it would
hide the real problem. You can pass the specific error types that mean "the
dependency failed" (say, your client's `TimeoutError` and `ServerError`), plus
an `exclude` list for cases like HTTP 4xx that are the caller's fault and should
never open the circuit.

**Why the clock is injectable.** Everything time-dependent goes through a
`clock` callable that defaults to `time.monotonic`. Tests pass a fake clock and
advance it by hand, so the whole recovery path is verified with zero real sleeps
and zero flakiness. Monotonic time also means a wall-clock adjustment (NTP, DST)
can never make the cooldown misbehave.

**Why half-open limits concurrency.** When the cooldown ends you want to know if
the service is back, but you do not want a thundering herd of queued callers all
retrying at once and knocking it over again. Half-open admits a bounded number
of probes and rejects the rest until it has an answer.

**Why the state-change callback is generic and its errors are swallowed.**
Wiring in logs, metrics, or alerts is a one-line `on_state_change` hook, and the
library knows nothing about your observability stack. If that callback throws,
the breaker suppresses it: a broken metrics exporter must never take down the
call path it is only supposed to observe.

**Why it is thread-safe.** Web apps and workers call shared dependencies from
many threads. All state transitions happen under a lock, so the failure count
and half-open slot accounting stay correct under concurrency. There is a test
that hammers one breaker from ten threads to prove it.

## Configuration

| Parameter | Default | What it does |
|---|---|---|
| `failure_threshold` | `5` | Consecutive failures in the closed state that trip the breaker open. |
| `recovery_timeout` | `30.0` | Seconds the breaker stays open before allowing trial calls. |
| `success_threshold` | `1` | Consecutive successes in half-open needed to close again. |
| `half_open_max_calls` | `1` | Maximum concurrent trial calls allowed while half-open. |
| `expected_exception` | `Exception` | Exception type(s) that count as a failure. |
| `exclude` | `None` | Exception type(s) that never count as a failure, even if they match `expected_exception`. |
| `name` | `"circuit"` | Label used in errors and passed to the callback. |
| `clock` | `time.monotonic` | Zero-arg callable returning seconds. Inject a fake in tests. |
| `on_state_change` | `None` | `fn(breaker, old_state, new_state)` called on every transition. |

Manual control is available too: `breaker.trip()` forces it open (handy from a
health check), `breaker.reset()` forces it closed, and `breaker.state` reports
the current state.

## Testing

```bash
pip install -e ".[test]"
pytest
```

The suite covers tripping, the full recovery path, success and failure
thresholds, exception filtering, the decorator and context-manager interfaces,
callback isolation, and concurrent access. Timing tests use an injected fake
clock, so they run instantly and never flake.

## License

MIT. See [LICENSE](LICENSE).
