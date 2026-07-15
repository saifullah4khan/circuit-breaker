"""A runnable demo of the circuit breaker walking through every state.

Run it:  python examples/demo.py

It uses a fake clock so the whole open -> half-open -> closed cycle plays out
instantly, with no real waiting.
"""

from circuit_breaker import CircuitBreaker, CircuitOpenError, State


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FlakyService:
    """A pretend dependency that is down until we tell it to recover."""

    def __init__(self):
        self.healthy = False

    def call(self):
        if not self.healthy:
            raise ConnectionError("service unavailable")
        return "200 OK"


def main():
    clock = FakeClock()
    service = FlakyService()

    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=10.0,
        name="demo-service",
        expected_exception=ConnectionError,
        clock=clock,
        on_state_change=lambda b, old, new: print(
            "  [state] {0} -> {1}".format(old, new)
        ),
    )

    print("1) Service is down. Watch the breaker trip after 3 failures.")
    for i in range(1, 5):
        try:
            breaker.call(service.call)
        except ConnectionError:
            print("  attempt {0}: failed (state={1})".format(i, breaker.state))
        except CircuitOpenError as e:
            print(
                "  attempt {0}: rejected fast, retry in {1:.0f}s".format(
                    i, e.retry_after
                )
            )

    print("\n2) Cooldown passes. The breaker probes once (half-open).")
    clock.advance(10)
    print("  state is now:", breaker.state)

    print("\n3) Service recovers. The next probe succeeds and closes the circuit.")
    service.healthy = True
    print("  probe result:", breaker.call(service.call))
    print("  state is now:", breaker.state)

    assert breaker.state is State.CLOSED
    print("\nDone. Back to normal operation.")


if __name__ == "__main__":
    main()
