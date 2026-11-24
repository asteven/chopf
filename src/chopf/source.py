from __future__ import annotations

import dataclasses
import logging
from typing import Any, Awaitable, Callable, Iterable, Optional, Sequence, TYPE_CHECKING

import anyio
from anyio import TASK_STATUS_IGNORED
from anyio.abc import TaskGroup, TaskStatus
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)

from lightkube.core import resource as lkr

from .tasks import Task
from .invocation import invoke

if TYPE_CHECKING:
    from .controller.request import Request
    from .workqueue import Workqueue

__all__ = [
    'EventSource',
]

log = logging.getLogger(__name__)

RequestIterable = Iterable['Request']
HandlerResult = Optional[RequestIterable]
HandlerCallable = Callable[..., HandlerResult | Awaitable[HandlerResult]]
PredicateCallable = Callable[[Any], bool | Awaitable[bool]]


@dataclasses.dataclass
class EventSource(Task):
    queue: 'Workqueue'
    resource: lkr.Resource
    handler: HandlerCallable
    kwargs: Optional[dict[str, Any]] = None
    predicates: Optional[Sequence[PredicateCallable]] = None

    def __post_init__(self) -> None:
        Task.__init__(self)
        self._task_group: TaskGroup | None = None  # Main taskgroup
        self._stop: anyio.Event = anyio.Event()
        self._informer_streams: dict[Any, Any] = {}
        self._streams: list[MemoryObjectSendStream[Any]] = []
        self._tx: MemoryObjectSendStream[Any] | None = None
        self._rx: MemoryObjectReceiveStream[Any] | None = None
        if self.kwargs is None:
            self.kwargs = {}

    def __repr__(self) -> str:
        api_version = self.resource._api_info.resource.api_version
        kind = self.resource._api_info.resource.kind
        return f'<{self.__class__.__name__} {api_version}/{kind}>'

    def __hash__(self) -> int:
        api_version = self.resource._api_info.resource.api_version
        kind = self.resource._api_info.resource.kind
        return hash((api_version, kind, self.handler))

    async def event_stream_handler(self) -> None:
        if self._rx is None:
            raise RuntimeError('event stream handler started before receiver was initialized')
        async with self._rx:
            async for event in self._rx:
                log.debug('received event: %s', event)
                should_run = True
                if self.predicates is not None:
                    for predicate in self.predicates:
                        predicate_result = await invoke(predicate, event)
                        if not predicate_result:
                            log.debug('predicate prevented event: %r', event)
                            should_run = False
                            break
                if should_run:
                    try:
                        requests = await invoke(self.handler, event, **self.kwargs)
                        if requests:
                            for request in requests:
                                await self.queue.add(request)
                    except Exception as e:  # noqa: BLE001
                        # TODO: proper error handling
                        log.error('FAILED to process %s', event)
                        if hasattr(event, 'obj'):
                            print(event.obj)
                        elif hasattr(event, 'old'):
                            print(event.old)
                            print(event.new)
                        raise e

    @property
    def stream(self) -> MemoryObjectSendStream[Any]:
        if self._tx is None:
            raise RuntimeError('event stream requested before transmitter was initialized')
        stream = self._tx.clone()
        self._streams.append(stream)
        return stream

    def stop(self) -> None:
        if self._task_group:
            self._task_group.cancel_scope.cancel()
        self.reset_task()

    async def __call__(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        log.debug('starting %s', self)

        try:
            self._tx, self._rx = anyio.create_memory_object_stream()
            async with anyio.create_task_group() as tg:
                self._task_group = tg
                try:
                    tg.start_soon(self.event_stream_handler)

                    log.debug('started %s', self)
                    # Inform any awaiters that we are ready.
                    task_status.started()
                    self._running.set()

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)
                    # Close all the clones of our inbound stream that we
                    # handed out.
                    for stream in self._streams:
                        stream.close()

        finally:
            log.debug('stopped %s', self)
