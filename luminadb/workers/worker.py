"""Worker thread"""

# pylint: disable=ungrouped-imports,possibly-used-before-assignment,too-few-public-methods,no-name-in-module,no-member

from sys import version_info
from concurrent.futures import Future
from queue import Empty, Queue
from sqlite3 import Connection
from threading import Thread, Event as EventThread
from typing import Literal, TypeAlias
from atexit import register as finalize
import warnings

from ..errors import Rejection, ImplementationWarning

WorkerType: TypeAlias = Literal["thread"] | Literal["process"]
POSSIBLE_STACKTRACE_COUNT = 6
FEATURE_MIN_VERSION = (3, 13)

if version_info >= FEATURE_MIN_VERSION:
    from queue import ShutDown  # type: ignore
else:
    class ShutDown(RuntimeError):
        """Raised when put/get with shut-down queue."""

class Worker:
    """Worker"""

    def __init__(self, *args, worker_type: WorkerType = "thread", **kwargs):
        self.conn = Connection(*args, **kwargs)
        self.queue = Queue()
        self.name = f"WorkerDB[{worker_type}]"
        self.daemon = False
        if worker_type in ("thread", "process"):
            self.event = EventThread()
            self.worker = Thread(target=self._run, name=self.name, daemon=self.daemon)

        if worker_type == 'process':
            warnings.warn(
                "Worker Process implementation is not supported, reverting back to threading.",
                ImplementationWarning,
                POSSIBLE_STACKTRACE_COUNT)
        self.accepting = self.event
        self._closing = False
        self.event.set()
        self.worker.start()
        finalize(self.close)

    def recall(self):
        """Recall remainding queues"""
        while True:
            try:
                fn, _, args, kwargs, fut = self.queue.get_nowait()
            except Empty:
                break
            try:
                if callable(fn):
                    res = fn(*args, **kwargs)
                    fut.set_result(res)
                else:
                    fut.set_result(None)
            except Exception as e:  # pylint: disable=broad-exception-caught
                fut.set_exception(e)
            finally:
                self.queue.task_done()

    def _run(self):
        while True:
            try:
                fn, owner, args, kwargs, fut = self.queue.get(timeout=0.01)
            except Empty:
                if self._closing:
                    break
                continue
            except ShutDown:
                break

            # If close signal is received, finish all tasks and quit
            if fn is None or (
                getattr(fn, "__name__", "") == "close" and owner == "connection"
            ):
                fut.set_result(None)
                self.queue.task_done()
                self._closing = True
                self.event.clear()
                self.recall()  # Finish all remaining tasks
                break

            try:
                if callable(fn):
                    res = fn(*args, **kwargs)
                    fut.set_result(res)
                else:
                    setattr(self.conn, fn, kwargs["value"])
                    fut.set_result(None)
            except Exception as e:  # pylint: disable=broad-exception-caught
                fut.set_exception(e)
            finally:
                self.queue.task_done()
        # Ensure connection is closed
        self.conn.close()

    def push(self, fn, owner: str, *args, **kwargs):
        """Push to worker"""
        if not self.accepting.is_set() or self._closing:
            exc = Rejection("Cannot push during shutdown")
            exc.add_note(f"Caller: {fn}")
            raise exc
        fut = Future()
        try:
            self.queue.put((fn, owner, args, kwargs, fut))
        except ShutDown:
            fut.set_result(None)
            return fut.result()
        return fut.result()  # blocks until worker finishes

    @property
    def is_closed(self):
        """Return true if worker is closed"""
        return self._closing

    def close(self, push=True):
        """Close this worker"""
        if self._closing:
            return
        self._closing = True
        self.event.clear()
        if push:
            # Push a close signal to the queue
            fut = Future()
            self.queue.put((lambda: None, "connection", (), {}, fut))
            fut.result()
        self.worker.join()

    def join(self, timeout: float = 0):
        """Join this worker"""
        self.worker.join(timeout)
