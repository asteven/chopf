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


def _create_event(event_name, args, kwargs):
    match event_name:
        case 'add':
            event = CreateEvent(args[0])
        case 'update':
            event = UpdateEvent(args[0], args[1])
        case 'delete':
            event = DeleteEvent(args[0])
    return event


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
        self._streams = {}

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

    def add_stream(self, stream, key=None):
        if key is None:
            key = stream
        self._streams[key] = stream

    def has_stream(self, stream=None, key=None):
        if key is None:
            key = stream
        return key in self._streams

    def remove_stream(self, stream=None, key=None):
        if key is None:
            key = stream
        if key in self._streams:
            del self._streams[key]

    def purge_streams(self):
        for stream in self._streams.values():
            stream.close()
        self._streams.clear()

    def add_indexers(self, indexers):
        self.store.add_indexers(indexers)

    def get_index(self, index_name):
        return self.store.get_index(
            index_name,
            resource=self.resource,
        )

    async def _stream_send(self, key, event):
        try:
            stream = self._streams[key]
            await stream.send(event)
        except (anyio.ClosedResourceError, anyio.BrokenResourceError) as e:
            #log.exception(e)
            #log.debug(f'key: {key}, stream: {stream} ')
            self.remove_stream(key)

    async def _dispatch(self, event_name, *args, **kwargs):
        """Create an event and dispatch it to all our streams."""
        #print(f'_dispatch: {event_name} {args}')
        if len(self._streams) > 0:
            event = _create_event(event_name, args, kwargs)

            # We iterate over a list of keys because the dict may change
            # while we're iterating over it.
            for key in list(self._streams.keys()):
                self._task_group.start_soon(self._stream_send, key, event)

    async def _add_or_update(self, obj):
        try:
            old = self.store.get(obj)
            if not is_same_version(obj, old):
                self.store.update(obj)
                await self._dispatch('update', old, obj)
        except KeyError:
            self.store.add(obj)
            await self._dispatch('add', obj)

    async def _delete(self, obj):
        self.store.delete(obj)
        await self._dispatch('delete', obj)

    async def _error(self, obj):
        # TODO: probably not used/needed -> remove.
        print(f'ERROR event handler called with: {obj}')
        await self._dispatch('error', obj)

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
            #print(f'_process_event: {event} {obj}')
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
        if self._task_group:
            self._task_group.cancel_scope.cancel()

    async def __call__(self):
        log.debug('starting %s', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    tg.start_soon(self._listwatch)

                    await self
                    log.info('started %s', self)

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)
                    self.purge_streams()

        finally:
            log.info('stopped %s', self)
