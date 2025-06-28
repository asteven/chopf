import dataclasses
import logging
import typing

import anyio

from lightkube.core import resource as lkr

from .tasks import Task
from .invocation import invoke


__all__ = [
    'EventSource',
]

log = logging.getLogger(__name__)


@dataclasses.dataclass
class EventSource(Task):
    queue: object
    resource: lkr.Resource
    handler: typing.Callable
    kwargs: dict = None
    predicates: typing.List[typing.Callable] = None

    def __post_init__(self):
        Task.__init__(self)
        self._task_group = None  # Main taskgroup
        self._stop = anyio.Event()
        self._informer_streams = {}
        self.tx, self.rx = anyio.create_memory_object_stream()

    def __repr__(self):
        api_version = self.resource._api_info.resource.api_version
        kind = self.resource._api_info.resource.kind
        return f'<{self.__class__.__name__} {api_version}/{kind}>'

    async def event_stream_handler(self):
        async with self.rx:
            async for event in self.rx:
                log.debug('received event: %s', event)
                should_run = True
                if self.predicates is not None:
                    for predicate in self.predicates:
                        if not await invoke(predicate, event):
                            log.debug('predicate prevented event: %r', event)
                            should_run = False
                            break
                if should_run:
                    try:
                        requests = await invoke(self.handler, event, **self.kwargs)
                        if requests:
                            for request in requests:
                                await self.queue.add(request)
                    except Exception:
                        # TODO: proper error handling
                        log.error(f'FAILED to process {event}')
                        if hasattr(event, 'obj'):
                            print(event.obj)
                        elif hasattr(event, 'old'):
                            print(event.old)
                            print(event.new)
                        raise

    def stop(self):
        if self._task_group:
            self._task_group.cancel_scope.cancel()

    def add_informer(self, informer):
        # Create a stream dedicated for the given informer.
        stream = self.tx.clone()
        self._informer_streams[informer] = stream
        informer.add_stream(stream)

    def remove_informer(self, informer):
        stream = self._informer_streams.pop(informer)
        informer.remove_stream(stream)
        stream.close()

    async def __call__(self):
        log.debug('starting %s', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    tg.start_soon(self.event_stream_handler)

                    log.debug('started %s', self)
                    # Inform any awaiters that we are ready.
                    self._running.set()

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)
                    # Remove our streams from the informers to which we added them.
                    for informer, stream in self._informer_streams.items():
                        informer.remove_stream(stream)
                        stream.close()

        finally:
            log.debug('stopped %s', self)
