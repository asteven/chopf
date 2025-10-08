import logging

import anyio
from lightkube.resources.core_v1 import Namespace

from ...tasks import Task
from ...cache import Indexer, Informer


log = logging.getLogger(__name__)


class NamespaceObserver(Task):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.resource = Namespace
        self.store = Indexer()
        self.informer = Informer(
            self.manager.async_api_client,
            self.store,
            self.resource,
        )
        self.tx, self.rx = anyio.create_memory_object_stream()
        self._stop = anyio.Event()

    async def event_stream_handler(self):
        async with self.rx:
            async for event in self.rx:
                match type(event):
                    case event.CreateEvent:
                        namespace = event.obj.metadata.name
                        if namespace in self.manager.namespaces:
                            await self.manager.add_namespace(namespace)
                        else:
                            await self.manager.remove_namespace(namespace)
                    case event.DeleteEvent:
                        namespace = event.obj.metadata.name
                        await self.manager.remove_namespace(namespace)

    async def __call__(self):
        log.debug('starting namespace observer')

        try:
            async with anyio.create_task_group() as tg:
                try:
                    tg.start_soon(self.event_stream_handler)

                    # Add our stream to the informer.
                    self.informer.add_stream(self.tx)

                    # Start the informer.
                    tg.start_soon(self.informer)
                    await self.informer

                    log.debug('started namespace observer')
                    # Inform any awaiters that we are ready.
                    self._running.set()

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled namespace observer')
                    raise

                finally:
                    log.debug('stopping namespace observer')
                    # Remove our stream from the informer.
                    self.informer.remove_stream(self.tx)

        finally:
            log.debug('stopped namespace observer')

