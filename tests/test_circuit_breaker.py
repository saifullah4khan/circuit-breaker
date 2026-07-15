"""Deterministic tests for the circuit breaker.

Every timing-dependent test uses a FakeClock so there are no real sleeps and no
flakiness. Run with: pytest
"""

import threading

import pytest

from circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerError,
    CircuitOpenError,
    State,
)


class FakeClock:
    """A manually advanced monotonic clock."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Boom(Exception):
    pass


def _fail():
    raise Boom("dependency down")


def _ok():
    return "ok"


# ---- construction --------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"failure_threshold": 0},
        {"success_threshold": 0},
        {"half_open_max_calls": 0},
        {"recovery_timeout": -1},
    ],
)
def test_invalid_config_rejected(kwargs):
    with pytest.raises(ValueError):
        CircuitBreaker(**kwargs)


def test_starts_closed():
    cb = CircuitBreaker()
    assert cb.state is State.CLOSED
    assert cb.failure_count == 0


# ---- tripping open -------------------------------------------------------


def test_opens_after_threshold_consecutive_failures():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(2):
        with pytest.raises(Boom):
            cb.call(_fail)
    assert cb.state is State.CLOSED  # not tripped yet
    with pytest.raises(Boom):
        cb.call(_fail)
    assert cb.state is State.OPEN


def test_success_resets_failure_streak():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(2):
        with pytest.raises(Boom):
            cb.call(_fail)
    cb.call(_ok)  # streak broken
    assert cb.failure_count == 0
    for _ in range(2):
        with pytest.raises(Boom):
            cb.call(_fail)
    assert cb.state is State.CLOSED


def test_open_rejects_calls_without_invoking_func():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10)
    with pytest.raises(Boom):
        cb.call(_fail)
    assert cb.state is State.OPEN

    calls = []

    def spy():
        calls.append(1)
        return "nope"

    with pytest.raises(CircuitOpenError):
        cb.call(spy)
    assert calls == []  # the protected function was never called


def test_circuit_open_error_carries_retry_after():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30, clock=clock)
    with pytest.raises(Boom):
        cb.call(_fail)
    clock.advance(10)
    with pytest.raises(CircuitOpenError) as ei:
        cb.call(_ok)
    assert ei.value.retry_after == pytest.approx(20.0)
    assert isinstance(ei.value, CircuitBreakerError)


# ---- recovery / half-open ------------------------------------------------


def test_transitions_to_half_open_after_timeout():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30, clock=clock)
    with pytest.raises(Boom):
        cb.call(_fail)
    assert cb.state is State.OPEN
    clock.advance(29)
    assert cb.state is State.OPEN  # cooldown not yet elapsed
    clock.advance(1)
    assert cb.state is State.HALF_OPEN  # exactly at recovery_timeout


def test_half_open_success_closes_circuit():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    with pytest.raises(Boom):
        cb.call(_fail)
    clock.advance(5)
    assert cb.call(_ok) == "ok"
    assert cb.state is State.CLOSED


def test_half_open_failure_reopens_and_extends_cooldown():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    with pytest.raises(Boom):
        cb.call(_fail)
    clock.advance(5)
    assert cb.state is State.HALF_OPEN
    with pytest.raises(Boom):
        cb.call(_fail)
    assert cb.state is State.OPEN
    # cooldown restarts from the moment of the half-open failure (now t=5),
    # so a full recovery_timeout must pass again before the next probe.
    clock.advance(4)
    assert cb.state is State.OPEN
    clock.advance(1)
    assert cb.state is State.HALF_OPEN


def test_success_threshold_requires_multiple_probes():
    clock = FakeClock()
    cb = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=5,
        success_threshold=2,
        half_open_max_calls=2,
        clock=clock,
    )
    with pytest.raises(Boom):
        cb.call(_fail)
    clock.advance(5)
    cb.call(_ok)
    assert cb.state is State.HALF_OPEN  # one more success needed
    cb.call(_ok)
    assert cb.state is State.CLOSED


def test_half_open_limits_concurrent_probes():
    clock = FakeClock()
    cb = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=5,
        half_open_max_calls=1,
        clock=clock,
    )
    with pytest.raises(Boom):
        cb.call(_fail)
    clock.advance(5)

    started = threading.Event()
    release = threading.Event()

    def slow():
        started.set()
        release.wait(timeout=2)
        return "ok"

    t = threading.Thread(target=lambda: cb.call(slow))
    t.start()
    assert started.wait(timeout=2)
    # The single probe slot is taken; a second caller is rejected.
    with pytest.raises(CircuitOpenError):
        cb.call(_ok)
    release.set()
    t.join(timeout=2)
    assert cb.state is State.CLOSED


# ---- which exceptions count ---------------------------------------------


def test_unexpected_exception_does_not_trip():
    cb = CircuitBreaker(failure_threshold=1, expected_exception=Boom)
    with pytest.raises(KeyError):
        cb.call(lambda: (_ for _ in ()).throw(KeyError("bug")))
    assert cb.state is State.CLOSED
    assert cb.failure_count == 0


def test_excluded_exception_does_not_trip():
    class ClientError(Boom):
        pass

    cb = CircuitBreaker(
        failure_threshold=1, expected_exception=Boom, exclude=ClientError
    )
    with pytest.raises(ClientError):
        cb.call(lambda: (_ for _ in ()).throw(ClientError("400")))
    assert cb.state is State.CLOSED


def test_uncounted_exception_frees_half_open_slot():
    clock = FakeClock()
    cb = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=5,
        expected_exception=Boom,
        clock=clock,
    )
    with pytest.raises(Boom):
        cb.call(_fail)
    clock.advance(5)
    assert cb.state is State.HALF_OPEN
    # A non-counting error while probing must not permanently consume the slot.
    with pytest.raises(KeyError):
        cb.call(lambda: (_ for _ in ()).throw(KeyError("bug")))
    assert cb.call(_ok) == "ok"
    assert cb.state is State.CLOSED


# ---- interfaces ----------------------------------------------------------


def test_decorator_interface():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10)

    @cb
    def flaky(x):
        if x < 0:
            raise Boom("negative")
        return x * 2

    assert flaky(3) == 6
    with pytest.raises(Boom):
        flaky(-1)
    with pytest.raises(CircuitOpenError):
        flaky(10)
    assert flaky.circuit_breaker is cb


def test_context_manager_records_failure():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10)
    with pytest.raises(Boom):
        with cb:
            raise Boom("down")
    assert cb.state is State.OPEN
    with pytest.raises(CircuitOpenError):
        with cb:
            pass  # not reached; entering raises


def test_context_manager_records_success():
    cb = CircuitBreaker(failure_threshold=2)
    with cb:
        pass
    assert cb.state is State.CLOSED
    assert cb.failure_count == 0


# ---- callbacks and manual control ---------------------------------------


def test_state_change_callback_fires():
    events = []
    cb = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=1,
        clock=FakeClock(),
        on_state_change=lambda b, old, new: events.append((old, new)),
    )
    with pytest.raises(Boom):
        cb.call(_fail)
    assert events == [(State.CLOSED, State.OPEN)]


def test_broken_callback_is_suppressed():
    def bad(b, old, new):
        raise RuntimeError("observer blew up")

    cb = CircuitBreaker(failure_threshold=1, on_state_change=bad)
    # The failing callback must not mask the original Boom.
    with pytest.raises(Boom):
        cb.call(_fail)
    assert cb.state is State.OPEN


def test_trip_and_reset():
    cb = CircuitBreaker()
    cb.trip()
    assert cb.state is State.OPEN
    cb.reset()
    assert cb.state is State.CLOSED
    assert cb.failure_count == 0


def test_thread_safety_under_concurrent_failures():
    cb = CircuitBreaker(failure_threshold=50, recovery_timeout=100)

    def hammer():
        for _ in range(20):
            try:
                cb.call(_fail)
            except (Boom, CircuitOpenError):
                pass

    threads = [threading.Thread(target=hammer) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # With 200 attempted failures and a threshold of 50, the breaker must end
    # up OPEN and its internal counters must not have gone out of range.
    assert cb.state is State.OPEN
