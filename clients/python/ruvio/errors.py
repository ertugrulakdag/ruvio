# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Exception types raised by the Ruvio client."""

from __future__ import annotations

__all__ = [
    "RuvioError",
    "ConnectionError",
    "TimeoutError",
    "ProtocolError",
    "ResponseError",
    "WrongTypeError",
    "NoScriptError",
    "NoGroupError",
    "BusyError",
    "ExecAbortError",
    "MovedError",
    "CrossSlotError",
    "LockError",
    "WatchError",
]


class RuvioError(Exception):
    """Base class for every error raised by this package."""


class ConnectionError(RuvioError, OSError):  # noqa: A001 - mirrors redis-py naming
    """The socket could not be opened, or it closed while a reply was pending."""


class TimeoutError(ConnectionError):  # noqa: A001
    """A socket operation timed out."""


class ProtocolError(RuvioError):
    """The server sent bytes that are not valid RESP."""


class ResponseError(RuvioError):
    """A server ``-ERR`` reply. ``code`` is the first word (``ERR``, ``WRONGTYPE`` ...)."""

    __slots__ = ("code",)

    def __init__(self, message: str) -> None:
        super().__init__(message)
        space = message.find(" ")
        self.code = message if space < 0 else message[:space]

    @property
    def message(self) -> str:
        return str(self.args[0])


class WrongTypeError(ResponseError):
    """``WRONGTYPE`` - the key holds another type."""


class NoScriptError(ResponseError):
    """``NOSCRIPT`` - EVALSHA for an unknown script."""


class NoGroupError(ResponseError):
    """``NOGROUP`` - the stream consumer group does not exist."""


class BusyError(ResponseError):
    """``BUSY`` / ``BUSYGROUP``."""


class ExecAbortError(ResponseError):
    """``EXECABORT`` - a queued command was refused, the transaction did not run."""


class MovedError(ResponseError):
    """``MOVED`` / ``ASK`` cluster redirection."""


class CrossSlotError(ResponseError):
    """``CROSSSLOT`` - the keys of one command (or transaction) live in different hash slots.

    Raised by the cluster client before anything is sent, and also if a server answers it."""

    def __init__(self, message: str = "CROSSSLOT Keys in request don't hash to the same slot") -> None:
        super().__init__(message)


class LockError(RuvioError):
    """A lock / lease / semaphore helper could not acquire or no longer holds the resource."""


_BY_CODE = {
    "WRONGTYPE": WrongTypeError,
    "NOSCRIPT": NoScriptError,
    "NOGROUP": NoGroupError,
    "BUSY": BusyError,
    "BUSYGROUP": BusyError,
    "EXECABORT": ExecAbortError,
    "MOVED": MovedError,
    "ASK": MovedError,
    "CROSSSLOT": CrossSlotError,
}


def make_error(message: str) -> ResponseError:
    """Builds the most specific :class:`ResponseError` subclass for ``message``."""
    space = message.find(" ")
    cls = _BY_CODE.get(message if space < 0 else message[:space], ResponseError)
    return cls(message)


class WatchError(RuvioError):
    """``EXEC`` returned nil: a watched key changed, the transaction did not run."""
