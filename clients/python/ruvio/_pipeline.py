# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Pipeline / transaction command queue shared by the sync and asyncio clients.
"""

from __future__ import annotations

from typing import Any, List, Optional

from ._commands import Commands, Conv
from ._resp import encode
from .errors import ExecAbortError, ResponseError, WatchError

_MULTI = encode("MULTI", ())
_EXEC = encode("EXEC", ())


class PipelineBase(Commands):
    """Queues typed commands; every command method returns the pipeline for chaining.

    Has every method of :class:`ruvio._commands.Commands`. Nothing is sent until ``execute()``. With
    ``transaction=True`` the batch is wrapped in ``MULTI``/``EXEC``, so it runs atomically on one shard.
    ``len(pipe)`` gives the number of queued commands.
    """

    __slots__ = ("_client", "_cmds", "_convs", "_transaction")

    def __init__(self, client: Any, transaction: bool) -> None:
        """Creates an empty pipeline. Use ``client.pipeline()`` instead.

        Args:
            client: The client that sends the batch.
            transaction: Wrap the batch in ``MULTI``/``EXEC``.
        """
        self._client = client
        self._cmds: List[bytes] = []
        self._convs: List[Conv] = []
        self._transaction = transaction

    def _x(self, conv: Conv, data: bytes, decode: Optional[bool] = None) -> Any:
        self._cmds.append(data)
        self._convs.append(conv)
        return self

    def __len__(self) -> int:
        return len(self._cmds)

    def reset(self) -> None:
        """Discards all queued commands without sending them."""
        self._cmds = []
        self._convs = []

    def _payload(self) -> List[bytes]:
        if self._transaction:
            return [_MULTI, *self._cmds, _EXEC]
        return self._cmds

    def _finish(self, raw: List[Any], raise_on_error: bool) -> List[Any]:
        convs = self._convs
        if self._transaction:
            queued = raw[1:-1]
            result = raw[-1]
            if isinstance(raw[0], ResponseError):
                raise raw[0]
            if isinstance(result, ResponseError):
                first = next((r for r in queued if isinstance(r, ResponseError)), None)
                if first is not None and not isinstance(result, ExecAbortError):
                    raise first
                raise ExecAbortError(f"{result.message}" + (f" ({first.message})" if first is not None else ""))
            if result is None:
                raise WatchError("transaction aborted: a watched key changed")
            if len(result) != len(queued):
                # Ruvio refuses a bad command at queue time and still runs the rest:
                # EXEC answers only for the queued ones; keep the refusals in place.
                it = iter(result)
                result = [q if isinstance(q, ResponseError) else next(it, None) for q in queued]
            raw = result
        out: List[Any] = []
        first_error: Optional[BaseException] = None
        for value, conv in zip(raw, convs):
            if isinstance(value, ResponseError):
                if first_error is None:
                    first_error = value
                out.append(value)
            elif conv is None:
                out.append(value)
            else:
                try:
                    out.append(conv(value))
                except Exception as exc:  # noqa: BLE001 - surfaced per command
                    if first_error is None:
                        first_error = exc
                    out.append(exc)
        if raise_on_error and first_error is not None:
            raise first_error
        return out
