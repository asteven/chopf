import dataclasses
import itertools
import logging
import random
import typing

import anyio
import httpx

from lightkube.core import resource as lkr
import lightkube

from ..exceptions import HttpError
from ..tasks import Task
from ..invocation import invoke
from ..resources import is_same_version
from . import CreateEvent, UpdateEvent, DeleteEvent


log = logging.getLogger(__name__)


class EventHandlerTask(Task):
    def __init__(self, handler, args, kwargs):
        super().__init__()
        self.handler = handler
        self.args = args
        self.kwargs = kwargs
        buffer_size = kwargs.get('buffer_size', 0)
        self._tx, self._rx = anyio.create_memory_object_stream(buffer_size)

    def __repr__(self):
        return f'{self.__class__.__name__} {self.handler} {self.args} {self.kwargs}'

    def invoke(self, *args, **kwargs):
        try:
            self._tx.send_nowait((args, kwargs))
        except anyio.WouldBlock:
            log.warn('warning, receiver would block, ignoring event')

    async def dispatch(self, *args, **kwargs):
        await self._tx.send((args, kwargs))

    async def __call__(self):
        self._running.set()
        async with self._rx:
            async for args, kwargs in self._rx:
                await invoke(self.handler, *args, **kwargs)

    def stop(self):
        self._tx.close()
        self._rx.close()


class EventManager:
    def __init__(self):
        super().__init__()
        self._handlers = {}
        self._streams = []

    def __repr__(self):
        _id = id(self)
        return f'<{self.__class__.__name__} {_id} {self._handlers}>'

    def add_stream(self, stream):
        self._streams.append(stream)

    def remove_stream(self, stream):
        if stream in self._streams:
            self._streams.remove(stream)

    def purge_streams(self):
        for stream in self._streams:
            stream.close()
        del self._streams[:]

    def add_handler(self, event, handler):
        handlers = self._handlers.get(event, None)
        if handlers is None:
            handlers = []
            self._handlers[event] = handlers
        handlers.append(handler)

    def _create_event(self, event_name, args, kwargs):
        match event_name:
            case 'add':
                event = CreateEvent(args[0])
            case 'update':
                event = UpdateEvent(args[0], args[1])
            case 'delete':
                event = DeleteEvent(args[0])
        return event

    async def dispatch(self, event_name, *args, **kwargs):
        if len(self._streams) > 0:
            event = self._create_event(event_name, args, kwargs)

        async with anyio.create_task_group() as tg:
            # Dispatch to streams.
            for stream in self._streams:
                tg.start_soon(stream.send, event)
            # Dispatch to event handlers.
            if event_name in self._handlers:
                for handler in self._handlers[event_name]:
                    handler.invoke(*args, **kwargs)

    def sync_dispatch(self, event_name, *args, **kwargs):
        if len(self._streams) > 0:
            event = self._create_event(event_name, args, kwargs)
            # Dispatch to streams.
            for stream in self._streams:
                try:
                    stream.send_nowait(event)
                except anyio.WouldBlock:
                    log.warn(f'warning, receiver would block, ignoring event {event}')
        # Dispatch to event handlers.
        if event_name in self._handlers:
            for handler in self._handlers[event_name]:
                handler.invoke(*args, **kwargs)

    async def __call__(self):
        async with anyio.create_task_group() as tg:
            for handler in itertools.chain(*self._handlers.values()):
                tg.start_soon(handler)
                await handler


@dataclasses.dataclass
class Informer(Task):
    api_client: object
    store: object
    resource: lkr.Resource
    namespace: str = None
    name: str = None
    resync_after: int = 10 * 60 * 60 + 60 * random.randint(
        0, 9
    )  # 10 hours + 0..9 Minutes
    timeout: int = 60
    transformer: typing.Callable = None
    resource_version: str = None

    @property
    def api_version(self) -> str:
        return self.resource._api_info.resource.api_version

    @property
    def kind(self) -> str:
        return self.resource._api_info.resource.kind

    def __post_init__(self):
        super().__init__()
        self._task_group = None  # Main taskgroup
        self._stop = anyio.Event()
        self._event_manager = EventManager()

    def __hash__(self):
        return hash((self.api_version, self.kind, self.namespace, self.name))

    def __repr__(self):
        _out = []
        _out.append(str(id(self)))
        if self.name is not None:
            _out.append(self.name)
        _out.append(f'{self.api_version}/{self.kind}')
        if self.namespace is not None:
            _out.append(self.namespace)
        if self.resource_version:
            _out.append(self.resource_version)
        _s = ' '.join(_out)
        return f'<Informer {_s}>'

    def add_stream(self, stream):
        self._event_manager.add_stream(stream)

    def remove_stream(self, stream):
        self._event_manager.remove_stream(stream)

    def purge_streams(self):
        self._event_manager.purge_streams()

    def add_event_handler(self, event, func, args, kwargs):
        handler = EventHandlerTask(func, args, kwargs)
        self._event_manager.add_handler(event, handler)

    def add_indexers(self, indexers):
        self.store.add_indexers(indexers)

    def get_index(self, index_name):
        return self.store.get_index(
            index_name,
            resource=self.resource,
        )

    async def _add_or_update(self, obj):
        try:
            old = self.store.get(obj)
            if not is_same_version(obj, old):
                self.store.update(obj)
                await self._event_manager.dispatch('update', old, obj)
        except KeyError:
            self.store.add(obj)
            await self._event_manager.dispatch('add', obj)

    async def _delete(self, obj):
        self.store.delete(obj)
        await self._event_manager.dispatch('delete', obj)

    async def _error(self, obj):
        # TODO: probably not used/needed -> remove.
        print(f'ERROR event handler called with: {obj}')
        self._event_manager.dispatch('error', obj)

    async def _list(self):
        log.debug('start listing %s/%s', self.api_version, self.kind)
        # Get initial resource version.
        try:
            with anyio.fail_after(self.timeout):
                async for obj in (
                    resource_list := self.api_client.list(
                        self.resource, namespace=self.namespace
                    )
                ):
                    self.resource_version = resource_list.resourceVersion
                    await self._process_event('LISTED', obj)
                log.debug(
                    'done listing %s/%s %s',
                    self.api_version,
                    self.kind,
                    self.resource_version,
                )
        except httpx.HTTPStatusError as e:
            raise HttpError(
                e.request.method,
                e.request.url,
                e.response.status_code,
                message=f'HTTP error while listing {self.api_version}/{self.kind}'
            ) from e
        except TimeoutError as e:
            raise TimeoutError(
                f'TimeoutError while listing {self.api_version}/{self.kind}'
            ) from e

    async def _watch(self):
        log.debug(
            'start watching %s/%s %s',
            self.api_version,
            self.kind,
            self.resource_version,
        )

        # TODO: instead of True, use a cache-resync flag here that can be
        #       toggled on/off from the outside.
        while True:
            try:
                # TODO: add support for timeout
                async for event, obj in self.api_client.watch(
                    self.resource,
                    resource_version=self.resource_version,
                    namespace=self.namespace,
                ):
                    await self._process_event(event, obj)
            except httpx.HTTPStatusError as e:
                raise HttpError(
                    e.request.method,
                    e.request.url,
                    e.response.status_code,
                    message=f'HTTP error while watching {self.api_version}/{self.kind}'
                ) from e
            except TimeoutError as e:
                raise TimeoutError(
                    f'TimeoutError while watching {self.api_version}/{self.kind}'
                ) from e

    async def _listwatch(self):
        while True:
            try:
                # Initial listing.
                await self._list()

                # We are running and our cache is synced.
                self._running.set()

                # Continue watching for changes.
                if self.resync_after is None:
                    await self._watch()
                else:
                    with anyio.move_on_after(self.resync_after) as scope:
                        await self._watch()

                    if scope.cancelled_caught:
                        log.debug(
                            'resyncing %s/%s %s',
                            self.api_version,
                            self.kind,
                            self.resource_version,
                        )

            except TimeoutError as e:
                # TODO: how do we deal with this? exponential backoff?
                log.error('Informer._listwatch timeout error')
                log.error(e)
            except lightkube.ApiError as e:
                log.error('Informer._listwatch ApiError')
                log.error(e)

    async def _process_event(self, event, obj):
        try:
            if callable(self.transformer):
                obj = self.transformer(obj)
            match event:
                case 'ADDED' | 'LISTED' | 'MODIFIED':
                    await self._add_or_update(obj)
                case 'DELETED':
                    await self._delete(obj)
                case 'ERROR':
                    await self._error(obj)
            # self.resource_version = obj.metadata.resourceVersion
        except Exception as e:
            # TODO: use a more explicit Exception subclass?
            raise e
            await self._error(obj)

    def stop(self):
        self._task_group.cancel_scope.cancel()

    async def __call__(self):
        log.debug('starting %s', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    tg.start_soon(self._event_manager)
                    tg.start_soon(self._listwatch)

                    await self
                    log.debug('started %s', self)

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)
                    self.purge_streams()

        finally:
            log.debug('stopped %s', self)
