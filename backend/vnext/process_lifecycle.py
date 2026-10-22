"""Small process-lifecycle guards for VNext child processes."""

from __future__ import annotations

import ctypes
import os
import signal
import sys


_PR_SET_PDEATHSIG = 1


def disable_kernel_core_dump() -> bool:
    """Keep a crashing Linux child from waiting on a piped core collector.

    RLIMIT_CORE is ignored for piped core_pattern handlers. PR_SET_DUMPABLE
    also covers that case; Python faulthandler remains available. This changes
    only the calling child, never the parent's or host's crash policy.
    """
    if sys.platform != "linux":
        return False
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        return libc.prctl(4, 0, 0, 0, 0) == 0  # PR_SET_DUMPABLE
    except (AttributeError, OSError, TypeError, ValueError):
        return False


def set_parent_death_signal(signum: int = signal.SIGTERM) -> bool:
    """Ask Linux to signal this child when its owning parent disappears.

    ``multiprocessing`` daemon processes are not automatically reaped when a
    native crash kills the parent.  The VNext workers own sockets and the
    projection writer owns a DuckDB file, so an orphan can keep processing or
    hold a file lock after the web process has gone away.  This is a no-op on
    non-Linux platforms and deliberately has no hot-path cost after startup.
    """

    if sys.platform != "linux":
        return False
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        prctl = libc.prctl
        prctl.argtypes = [
            ctypes.c_int,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
            ctypes.c_ulong,
        ]
        prctl.restype = ctypes.c_int
        result = prctl(_PR_SET_PDEATHSIG, int(signum), 0, 0, 0)
        if result != 0:
            return False

        # Close the small race where the parent exits between process creation
        # and prctl().  The kernel handles the normal post-prctl case.
        if os.getppid() == 1:
            os.kill(os.getpid(), int(signum))
        return True
    except (AttributeError, OSError, TypeError, ValueError):
        return False
