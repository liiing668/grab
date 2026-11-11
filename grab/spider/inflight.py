"""
In-flight capacity limiting for the Spider.

The limiter bounds the number of task attempts whose downloaded
(or cache-loaded) content is being held in memory at the same time.

A slot (see `InFlightToken`) is acquired right before an attempt to
read the task content from the cache or from the network. The slot is
held until both:

* the output of the task handler (the parse callback) has been
  completely consumed, and
* the cache write of the downloaded response (if any) has finished.

Failed attempts release their slot immediately; a retry acquires a
new slot when it re-enters the cache reader or the network service.
A cache miss followed by a network request does NOT acquire a second
slot: the token travels with the task object.
"""
from threading import Condition, Lock


class InFlightLimiter(object):
    """
    A semaphore-like primitive which bounds the number of in-flight
    task attempts of a single Spider instance.

    `acquire()` blocks while `size` slots are in use. `stop()` wakes
    up all waiting threads and makes `acquire()` return False, so
    services blocked on the capacity can shut down promptly.
    """

    def __init__(self, size):
        self.size = size
        self._cond = Condition()
        self._inflight = 0
        self._stopped = False

    @property
    def inflight(self):
        """Number of slots currently in use."""
        with self._cond:
            return self._inflight

    @property
    def stopped(self):
        with self._cond:
            return self._stopped

    def acquire(self):
        """
        Take one slot, waiting if all slots are in use.

        Returns True if the slot has been acquired, False if the
        limiter has been stopped while waiting.
        """
        with self._cond:
            while self._inflight >= self.size and not self._stopped:
                self._cond.wait()
            if self._stopped:
                return False
            self._inflight += 1
            return True

    def release(self):
        with self._cond:
            self._inflight -= 1
            self._cond.notify()

    def stop(self):
        """Wake up all waiting threads; further acquires fail."""
        with self._cond:
            self._stopped = True
            self._cond.notify_all()


class InFlightToken(object):
    """
    Represents one occupied slot of `InFlightLimiter`.

    The token is released when all expected completion events have
    been reported:

    * `finish_parse()` - the task handler output has been exhausted
      (always expected, hence `pending` starts at 1),
    * `finish_cache()` - the cache write of the response has finished
      (expected only if `expect_cache_write()` was called).

    `release()` drops the slot immediately regardless of pending
    events; it is used for failed/aborted attempts. All methods are
    idempotent and thread-safe: the slot is returned to the limiter
    at most once.
    """

    def __init__(self, limiter):
        self._limiter = limiter
        self._lock = Lock()
        self._pending = 1  # the parse completion event
        self._released = False

    def expect_cache_write(self):
        """Declare that a cache write completion is also expected."""
        with self._lock:
            if not self._released:
                self._pending += 1

    def finish_parse(self):
        self._finish_event()

    def finish_cache(self):
        self._finish_event()

    def _finish_event(self):
        with self._lock:
            if self._released:
                return
            self._pending -= 1
            if self._pending <= 0:
                self._released = True
                self._limiter.release()

    def release(self):
        """Force-release the slot (failed/aborted attempt)."""
        with self._lock:
            if not self._released:
                self._released = True
                self._limiter.release()
