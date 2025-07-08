import functools
import logging
import platform
import signal
import uuid

import uvloop
import anyio
from anyio import open_signal_receiver
from anyio.abc import CancelScope

from lightkube import AsyncClient as LightkubeAsyncClient
from lightkube import Client as LightkubeSyncClient
from lightkube.resources.core_v1 import Namespace
from lightkube.generic_resource import async_load_in_cluster_generic_resources



from ..cache import Cache, Indexer, Informer
from ..controller import Controller
from ..client import AsyncClient, SyncClient
from ..tasks import Task

from .builders import ControllerBuilder, InformerBuilder, StoreBuilder


log = logging.getLogger(__name__)


async def signal_handler(scope: CancelScope):
    with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                print('Ctrl+C pressed!')
            else:
                print('Terminated!')

            scope.cancel()
            return


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


class Manager:
    def __init__(self):
        self.namespaces = None
        self.all_namespaces = False
        self.async_api_client = LightkubeAsyncClient()
        self.sync_api_client = LightkubeSyncClient()
        self.async_client = None
        self.sync_client = None
        self.cache = None
        self._builders = {
            'controller': {},
            'informer': {},
            'store': {},
        }
        self._main_task_group = None  # Main taskgroup
        self._task_group = None
        self._stop = None
        self._exit = anyio.Event()
        self._namespace_observer = None
        self.is_active = False
        self._active_namespaces = []
        # The unique ID of this operator, used in leader election.
        self.operator_id = '%s-%s' % (
            platform.node(),
            str(uuid.uuid4()).replace('-', '')[:10],
        )

    def __repr__(self):
        _id = id(self)
        namespaces = self._active_namespaces
        return f'<Manager {self.operator_id} namespaces: {namespaces}>'

    def run(self, all_namespaces=False, namespaces=None):
        anyio.run(
            functools.partial(
                self,
                all_namespaces=all_namespaces,
                namespaces=namespaces,
                setup_signal_handler=True,
            ),
            backend_options={'loop_factory': uvloop.new_event_loop},
        )

    async def add_namespace(self, namespace):
        log.debug('Manager add_namespace: %s', namespace)
        self._active_namespaces.append(namespace)
        if self.is_active:
            await self.cache.add_namespace(namespace)

    async def remove_namespace(self, namespace):
        log.debug('Manager remove_namespace: %s', namespace)
        if namespace in self._active_namespaces:
            self._active_namespaces.remove(namespace)
        if self.is_active:
            await self.cache.remove_namespace(namespace)

    def stop(self):
        log.debug('stop %r', self)
        self._stop.set()
        if self._task_group:
            self._task_group.cancel_scope.cancel()
        self.is_active = False

    async def start(self):
        log.debug('start %r', self)
        self._stop = anyio.Event()
        self._main_task_group.start_soon(self._start)
        self.is_active = True

    async def _start(self):
        log.debug('starting %s', self)
        self.cache = Cache(
            self.async_api_client,
            all_namespaces=self.all_namespaces,
            namespaces=self._active_namespaces,
        )

        self.async_client = AsyncClient(self.async_api_client, self.cache)
        self.sync_client = SyncClient(self.sync_api_client, self.cache)

        # TODO: better place to do this?
        await async_load_in_cluster_generic_resources(self.async_api_client)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg


                try:
                    for builder in self._builders['store'].values():
                        log.debug('creating store from builder: %r', builder)
                        store = self.cache.get_store(builder.resource)
                        store.add_indexers(builder._kwargs['indexers'])
                        # The builder needs an instance so it can proxy to it at runtime.
                        builder._instance = store

                    for builder in self._builders['controller'].values():
                        log.debug('creating controller from builder: %r', builder)
                        controller = Controller(
                            self.async_client,
                            self.sync_client,
                            self.cache,
                            builder.resource,
                            **builder._kwargs,
                        )
                        # The builder needs an instance so it can proxy to it at runtime.
                        builder._instance = controller
                        tg.start_soon(controller)
                        await controller.running

                    for builder in self._builders['informer'].values():
                        log.debug('creating informer from builder: %r', builder)
                        informer = self.cache.get_informer(
                            builder.resource,
                            builder.namespace,
                        )
                        # Connect the stream receivers that the builder collected.
                        for stream_receiver in builder._stream_receivers:
                            tx, rx = anyio.create_memory_object_stream()
                            tg.start_soon(stream_receiver, rx)
                            informer.add_stream(tx)
                        # Add the event handlers that the builder collected.
                        for event, callbacks in builder.on.items():
                            for callback in callbacks:
                                informer.add_event_handler(
                                    event, callback.func, callback.args, callback.kwargs
                                )
                        # The builder needs an instance so it can proxy to it at runtime.
                        builder._instance = informer

                    tg.start_soon(self.cache)
                    await self.cache

                    log.debug('started %s', self)

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)

        finally:
            log.debug('stopped %s', self)

    async def __call__(
        self, all_namespaces=False, namespaces=None, setup_signal_handler=False
    ):
        log.debug('startup %s', self)
        self.all_namespaces = all_namespaces
        if not self.all_namespaces:
            if namespaces is not None:
                self.namespaces = namespaces
            else:
                self.namespaces = [self.async_api_client.namespace]
            self._namespace_observer = NamespaceObserver(self)

        async with anyio.create_task_group() as tg:
            self._main_task_group = tg
            if setup_signal_handler:
                tg.start_soon(signal_handler, tg.cancel_scope)

            if self._namespace_observer:
                tg.start_soon(self._namespace_observer)
                await self._namespace_observer

            tg.start_soon(self.start)

            # Wait until told otherwise.
            await self._exit.wait()

        log.debug('exiting %s', self)

    def store(self, resource):
        """Decorator that creates and returns a store builder."""
        _builders = self._builders['store']
        key = resource
        builder = _builders.get(key, None)
        if builder is None:
            builder = StoreBuilder(
                self,
                resource,
            )
            _builders[key] = builder
        return builder

    def controller(self, resource, name=None):
        """Decorator that creates and returns a controller builder."""
        _builders = self._builders['controller']
        if name is not None:
            key = name
        else:
            key = resource
        builder = _builders.get(key, None)
        if builder is None:
            builder = ControllerBuilder(
                self,
                resource,
                name=name,
            )
            _builders[key] = builder
        return builder

    def informer(self, resource, namespace=None, name=None):
        """Decorator that creates and returns a informer builder."""
        _builders = self._builders['informer']
        if name is not None:
            key = name
        else:
            key = (resource, namespace)
        builder = _builders.get(key, None)
        if builder is None:
            builder = InformerBuilder(self, resource, namespace=namespace, name=name)
            _builders[key] = builder
        return builder
