"""A small, dependency-free circuit breaker for Python.

The circuit breaker stops an application from repeatedly calling a dependency
that is already failing. After a run of failures it "opens" and rejects calls
immediately for a cooldown window, then allows a few trial calls through to see
whether the dependency has recovered.

Public API:
    CircuitBreaker      the breaker itself (use .call(), the decorator, or a
                        context manager)
    CircuitOpenError    raised when a call is rejected because the circuit is open
    CircuitBreakerError base class for this package's errors
    State               CLOSED / OPEN / HALF_OPEN
"""

from __future__ import annotations

import functools
import threading
import time
from enum import Enum
from typing import Any, Callable, Iterable, Optional, Tuple, Type, TypeVar

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerError",
    "CircuitOpenError",
    "State",
]

__version__ = "0.1.0"

T = TypeVar("T")

# A callback invoked on every state transition. Kept intentionally generic so
# callers can wire in logging, metrics, or alerts without this package knowing
# anything about them.
StateChangeCallback = Callable[["CircuitBreaker", "State", "State"], None]


class State(str, Enum):
    """The three states of the breaker.

    CLOSED    - normal operation, calls pass through.
    OPEN      - calls are rejected immediately without touching the dependency.
    HALF_OPEN - a limited number of trial calls are allowed to probe recovery.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return self.value


class CircuitBreakerError(Exception):
    """Base class for every error raised by this package."""


class CircuitOpenError(CircuitBreakerError):
    """Raised when a call is rejected because the circuit is open.

    ``retry_after`` is the number of seconds the caller should wait before the
    breaker will allow a trial call. It may be ``0.0`` if the cooldown has
    already elapsed but no other caller has triggered the transition yet.
    """

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = max(0.0, retry_after)
        super().__init__(
            "circuit '{0}' is open; retry after {1:.3f}s".format(
                name, self.retry_after
            )
        )


def _as_tuple(
    value: Optional[Iterable[Type[BaseException]]] | Type[BaseException],
) -> Tuple[Type[BaseException], ...]:
    if value is None:
        return ()
    if isinstance(value, type) and issubclass(value, BaseException):
        return (value,)
    return tuple(value)


class CircuitBreaker:
    """A thread-safe circuit breaker.

    Parameters
    ----------
    failure_threshold:
        Number of consecutive failures while CLOSED that trips the breaker OPEN.
    recovery_timeout:
        Seconds the breaker stays OPEN before allowing trial calls (HALF_OPEN).
    success_threshold:
        Number of consecutive successes required in HALF_OPEN to close again.
    half_open_max_calls:
        Maximum number of trial calls permitted concurrently while HALF_OPEN.
        Extra calls are rejected with ``CircuitOpenError`` until a slot frees up.
    expected_exception:
        Exception type(s) that count as failures. Anything else propagates
        without affecting breaker state (a ``ValueError`` from your own code is
        usually a bug, not a dependency outage).
    exclude:
        Exception type(s) that should never count as failures, even if they are
        subclasses of ``expected_exception``.
    name:
        A label used in error messages and passed to the state-change callback.
    clock:
        A zero-argument callable returning a monotonically increasing float
        (seconds). Defaults to ``time.monotonic``. Inject a fake clock to make
        time-dependent behaviour deterministic in tests.
    on_state_change:
        Optional callback ``fn(breaker, old_state, new_state)`` invoked on every
        transition. Exceptions raised by the callback are suppressed so a broken
        metrics hook can never take down the protected call path.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 1,
        half_open_max_calls: int = 1,
        expected_exception: Optional[
            Iterable[Type[BaseException]] | Type[BaseException]
        ] = Exception,
        exclude: Optional[
            Iterable[Type[BaseException]] | Type[BaseException]
        ] = None,
        name: str = "circuit",
        clock: Callable[[], float] = time.monotonic,
        on_state_change: Optional[StateChangeCallback] = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.half_open_max_calls = half_open_max_calls
        self._expected = _as_tuple(expected_exception) or (Exception,)
        self._exclude = _as_tuple(exclude)
        self.name = name
        self._clock = clock
        self._on_state_change = on_state_change

        self._lock = threading.RLock()
        self._state = State.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: Optional[float] = None
        # Number of trial calls currently allowed out while HALF_OPEN.
        self._half_open_in_flight = 0

    # ---- introspection -------------------------------------------------

    @property
    def state(self) -> State:
        """The current state, accounting for an elapsed cooldown.

        Reading the state can itself move the breaker from OPEN to HALF_OPEN if
        the recovery timeout has passed, so that monitoring code sees the same
        truth the call path does.
        """
        with self._lock:
            self._maybe_half_open()
            return self._state

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failure_count

    def _counts_as_failure(self, exc: BaseException) -> bool:
        if self._exclude and isinstance(exc, self._exclude):
            return False
        return isinstance(exc, self._expected)

    # ---- state transitions (call while holding the lock) ---------------

    def _transition(self, new_state: State) -> None:
        old_state = self._state
        if old_state == new_state:
            return
        self._state = new_state
        if new_state == State.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            self._opened_at = None
            self._half_open_in_flight = 0
        elif new_state == State.OPEN:
            self._opened_at = self._clock()
            self._success_count = 0
            self._half_open_in_flight = 0
        elif new_state == State.HALF_OPEN:
            self._success_count = 0
            self._half_open_in_flight = 0
        if self._on_state_change is not None:
            try:
                self._on_state_change(self, old_state, new_state)
            except Exception:
                # A misbehaving observer must never break the call path.
                pass

    def _maybe_half_open(self) -> None:
        if self._state != State.OPEN:
            return
        assert self._opened_at is not None
        if self._clock() - self._opened_at >= self.recovery_timeout:
            self._transition(State.HALF_OPEN)

    def _retry_after(self) -> float:
        if self._opened_at is None:
            return 0.0
        return self.recovery_timeout - (self._clock() - self._opened_at)

    # ---- the guard: acquire a slot before the protected call -----------

    def _before_call(self) -> None:
        with self._lock:
            self._maybe_half_open()
            if self._state == State.OPEN:
                raise CircuitOpenError(self.name, self._retry_after())
            if self._state == State.HALF_OPEN:
                if self._half_open_in_flight >= self.half_open_max_calls:
                    # We are already probing; reject extra callers rather than
                    # flooding a dependency that may still be down.
                    raise CircuitOpenError(self.name, self._retry_after())
                self._half_open_in_flight += 1

    def _on_success(self) -> None:
        with self._lock:
            if self._state == State.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._transition(State.CLOSED)
            elif self._state == State.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        with self._lock:
            if self._state == State.HALF_OPEN:
                # A single failure during probing sends us straight back to OPEN.
                self._transition(State.OPEN)
            elif self._state == State.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self.failure_threshold:
                    self._transition(State.OPEN)

    # ---- public entry points -------------------------------------------

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run ``func(*args, **kwargs)`` through the breaker.

        Raises ``CircuitOpenError`` immediately if the circuit is open. Otherwise
        the call runs; a failure (an exception matching ``expected_exception``)
        is recorded and re-raised, and a success is recorded before the result is
        returned.
        """
        self._before_call()
        try:
            result = func(*args, **kwargs)
        except BaseException as exc:
            if self._counts_as_failure(exc):
                self._on_failure()
            else:
                # Not a dependency failure: release any half-open slot we took
                # without counting it for or against recovery.
                self._release_uncounted()
            raise
        else:
            self._on_success()
            return result

    def _release_uncounted(self) -> None:
        with self._lock:
            if self._state == State.HALF_OPEN and self._half_open_in_flight > 0:
                self._half_open_in_flight -= 1

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Use the breaker as a decorator."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self.call(func, *args, **kwargs)

        wrapper.circuit_breaker = self  # type: ignore[attr-defined]
        return wrapper

    def __enter__(self) -> "CircuitBreaker":
        """Guard a block of code.

            with breaker:
                do_the_thing()

        Entering raises ``CircuitOpenError`` if the circuit is open. On exit, an
        exception matching ``expected_exception`` is recorded as a failure; a
        clean exit is recorded as a success.
        """
        self._before_call()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            self._on_success()
        elif self._counts_as_failure(exc):
            self._on_failure()
        else:
            self._release_uncounted()
        return False  # never suppress

    # ---- manual control ------------------------------------------------

    def reset(self) -> None:
        """Force the breaker back to CLOSED and clear all counters."""
        with self._lock:
            self._transition(State.CLOSED)

    def trip(self) -> None:
        """Force the breaker OPEN (for example, from a health check)."""
        with self._lock:
            self._transition(State.OPEN)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "CircuitBreaker(name={0!r}, state={1}, failures={2})".format(
            self.name, self._state.value, self._failure_count
        )
