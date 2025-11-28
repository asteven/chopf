import functools
import itertools
import logging
import platform
import signal
import sys
import uuid

import uvloop
import anyio
from anyio import open_signal_receiver
from anyio import TASK_STATUS_IGNORED
from anyio.abc import CancelScope, TaskStatus

from lightkube import AsyncClient as LightkubeAsyncClient
from lightkube import Client as LightkubeSyncClient
from lightkube.generic_resource import async_load_in_cluster_generic_resources, create_resources_from_crd
from lightkube.core.resource_registry import resource_registry, LoadResourceError


import chopf
from .. import exceptions
from ..cache import Cache, Indexer, Informer
from ..controller import Controller
from ..client import AsyncClient, SyncClient
from ..tasks import Task
from ..resources import get_resource
from ..builder import resource_rbac

from .builders import ControllerBuilder, InformerBuilder, StoreBuilder
from .controllers import CrdController, NamespaceController, LeaseController


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


class StreamReceiver(Task):
    def __init__(self, resource, handler):
        super().__init__()
        self.resource = resource
        self.handler = handler
        self._tx = None
        self._rx = None
        self._cancel_scope = None
        self._streams = []

    def __hash__(self):
        api_version = self.resource._api_info.resource.api_version
        kind = self.resource._api_info.resource.kind
        return hash((api_version, kind, self.handler))

    def __repr__(self):
        return f'<StreamReceiver {self.resource.apiVersion}/{self.resource.kind} {self.handler}>'

    @property
    def stream(self):
        stream = self._tx.clone()
        self._streams.append(stream)
        return stream

    def stop(self):
        if self._cancel_scope:
            log.debug('stop %s', self)
            self._cancel_scope.cancel()
        self.reset_task()

    async def __call__(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        log.debug('starting %s', self)

        try:
            self._tx, self._rx = anyio.create_memory_object_stream()
            with CancelScope() as self._cancel_scope:
                try:
                    log.debug('started %s', self)
                    task_status.started()
                    self._running.set()
                    await self.handler(self._rx)

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


class Manager:
    def __init__(self):
        self.all_namespaces = False
        self.namespaces = None
        self.resources = set()
        self.debug = False
        self.async_api_client = LightkubeAsyncClient()
        self.sync_api_client = LightkubeSyncClient()
        self.cache = Cache(
            self.async_api_client,
        )
        self.async_client = AsyncClient(self.async_api_client, self.cache)
        self.sync_client = SyncClient(self.sync_api_client, self.cache)
        self.is_active = False
        # The unique ID of this operator, used in leader election.
        self.operator_id = '%s-%s' % (
            platform.node(),
            str(uuid.uuid4()).replace('-', '')[:10],
        )
        self._builders = {
            'controller': {},
            'informer': {},
            'store': {},
        }
        # The namespace this manager runs in.
        self.manager_namespace = self.async_api_client.namespace
        self._active_namespaces = set()
        self._active_resources = set()
        self._controllers = {}
        self._stream_receivers = {}
        self._main_task_group = None  # Main taskgroup
        self._task_group = None # regular taskgroup
        self._stop = None
        self._exit = anyio.Event()

    def __repr__(self):
        _id = id(self)
        #namespaces = self._active_namespaces
        namespaces = self.namespaces
        resources = {'%s/%s' % (resource.apiVersion, resource.kind) for resource in self.resources}
        return f'<Manager {self.operator_id} namespaces: {namespaces} resources: {resources}>'

    def run(self, all_namespaces=False, namespaces=None, debug=False):
        self.debug = debug
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
        if not namespace in self._active_namespaces:
            log.info('namespace_added: %s', namespace)
            self._active_namespaces.add(namespace)
            await self.reconcile()

    async def remove_namespace(self, namespace):
        if namespace in self._active_namespaces:
            log.info('namespace_removed: %s', namespace)
            self._active_namespaces.remove(namespace)
            await self.reconcile()

    async def add_resource(self, resource=None, version=None, kind=None):
        if resource is None:
            resource = resource_registry.load(version, kind)
        if not resource in self._active_resources:
            log.info('resource_added: %s/%s', resource.apiVersion, resource.kind)
            self._active_resources.add(resource)
            await self.reconcile()

    async def remove_resource(self, resource=None, version=None, kind=None):
        if resource is None:
            resource = resource_registry.load(version, kind)
        if resource in self._active_resources:
            log.info('resource_removed: %s/%s', resource.apiVersion, resource.kind)
            self._active_resources.remove(resource)
            await self.reconcile()

    def stop(self):
        log.debug('stop %r', self)
        self._stop.set()
        if self._task_group:
            self._task_group.cancel_scope.cancel()
        self.is_active = False

    async def start(self):
        log.debug('start %r', self)
        self._stop = anyio.Event()
        await self._main_task_group.start(self._start)
        self.is_active = True

    def configure_cache(self):
        self.cache._all_namespaces = self.all_namespaces
        if not self.all_namespaces:
            self.cache.namespaces = self._active_namespaces.copy()
        self.cache.resources = self._active_resources.copy()

    async def reconcile(self, startup=False):
        log.debug(f'reconcile: is_active: {self.is_active},  startup={startup}')
        if not self.is_active and not startup:
            return

        self.configure_cache()

        self.cache.reconcile_informers()

        # Start or stop stream receivers and connect them to their informers.
        for resource, stream_receivers in self._stream_receivers.items():
            if resource in self._active_resources:
                for stream_receiver in stream_receivers:
                    if not stream_receiver.is_running:
                        await self._task_group.start(stream_receiver)
                        #self._task_group.start_soon(stream_receiver)
                for informer in self.cache.get_informers_by(resource=resource):
                    for stream_receiver in stream_receivers:
                        if not informer.has_stream(key=stream_receiver):
                            informer.add_stream(stream_receiver.stream, key=stream_receiver)
            else:
                for stream_receiver in stream_receivers:
                    stream_receiver.stop()

        # Start or stop controllers and connect them to their informers.
        for resource, controller in self._controllers.items():
            if resource in self._active_resources:
                if not controller.is_running:
                    await self._task_group.start(controller)
                #    #self._task_group.start_soon(controller)

                #for source in controller.event_sources:
                #    for informer in self.cache.get_informers_by(resource=source.resource):
                #        if not informer.has_stream(key=source):
                #            informer.add_stream(source.stream, key=source)
            else:
                controller.stop()

        await self.cache.reconcile()

    def add_stream_receiver(self, stream_receiver):
        try:
            _list = self._stream_receivers[stream_receiver.resource]
        except KeyError:
            _list = self._stream_receivers[stream_receiver.resource] = []
        _list.append(stream_receiver)

    async def _verify_resources(self):
        # Ensure all resources we intend to use are actually usable,
        # and ignore the ones which are not.
        for resource in self.resources:
            try:
                resource_registry.load(resource.apiVersion, resource.kind)
                await self.add_resource(resource)
            except LoadResourceError as e:
                log.warning('ignoring unusable resource: %s/%s',
                    resource.apiVersion,
                    resource.kind,
                )
                await self.remove_resource(resource)

    async def _start(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        log.debug('starting %s', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    # Verify resources.
                    await self._verify_resources()

                    # Initial cache config.
                    self.configure_cache()

                    # Start the cache.
                    tg.start_soon(self.cache)

                    # Initial reconcilation.
                    tg.start_soon(self.reconcile, True)

                    #log.debug('started %s', self)
                    log.info('started %s', self)
                    task_status.started()

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)

        finally:
            log.info('stopped %s', self)

    async def __call__(
        self, all_namespaces=False, namespaces=None, setup_signal_handler=False
    ):
        self.all_namespaces = all_namespaces
        if not self.all_namespaces:
            if namespaces is not None:
                self.namespaces = set(namespaces)
            else:
                self.namespaces = set([self.async_api_client.namespace])

        log.debug('startup %s', self)

        for builder in self._builders['informer'].values():
            log.debug('creating stream receivers from informer builder: %r', builder)
            for receiver in builder._stream_receivers:
                stream_receiver = StreamReceiver(builder.resource, receiver)
                self.add_stream_receiver(stream_receiver)

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
            self._controllers[builder.resource] = controller

        try:
            async with anyio.create_task_group() as tg:
                self._main_task_group = tg
                if setup_signal_handler:
                    tg.start_soon(signal_handler, tg.cancel_scope)

                if not self.all_namespaces:
                    # Start namespace controller.
                    await tg.start(self._start_controller,
                        NamespaceController,
                    )

                # Start custom resource controller.
                await tg.start(self._start_controller,
                    CrdController,
                )

                # Start leader election controller.
                await tg.start(functools.partial(
                    self._start_controller,
                    LeaseController,
                    namespace=self.manager_namespace,
                ))

                tg.start_soon(self.start)

                # Wait until told otherwise.
                await self._exit.wait()
        except* exceptions.Error as eg:
            if self.debug:
                raise eg
            error_messages = []
            for error in exceptions.iterate_errors(eg):
                error_messages.append(str(error))
            raise exceptions.FatalError(' '.join(error_messages)) from eg

        log.debug('exiting %s', self)

    async def _start_controller(self, controller_class, namespace=None, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        """Start the given controller."""
        controller = controller_class(
            self,
            self.async_client,
            self.sync_client,
            self.cache,
            wait_for_cache=False,
        )
        resource = controller.resource
        #store = self.cache.get_store(resource)
        store = Indexer()
        informer = Informer(
            self.async_api_client,
            store,
            resource,
            namespace=namespace,
        )

        # Start the controller.
        self._main_task_group.start_soon(controller)
        await controller

        # Connect the controllers sources to the informer.
        for source in controller.event_sources:
            if not informer.has_stream(key=source):
                informer.add_stream(source.stream, key=source)

        # Start the informer.
        self._main_task_group.start_soon(informer)
        await informer

        ## Wait until all initial requests are processed.
        while len(controller.queue) > 0:
            await anyio.sleep(1)

        task_status.started()

    def register_resource(self, resource):
        # Register minimal read-only RBAC so the informers and clients which
        # will be working with the resource can do their job.
        resource_rbac(resource, verbs='get;list;watch')
        resource = get_resource(resource)
        self.resources.add(resource)

    def store(self, resource):
        """Decorator that creates and returns a store builder."""
        _builders = self._builders['store']
        self.register_resource(resource)
        return self.cache.get_store(resource)

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
        self.register_resource(resource)
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

    def informer(self, resource, name=None):
        """Decorator that creates and returns a informer builder."""
        _builders = self._builders['informer']
        self.register_resource(resource)
        if name is not None:
            key = name
        else:
            key = resource
        builder = _builders.get(key, None)
        if builder is None:
            builder = InformerBuilder(self, resource, name=name)
            _builders[key] = builder
        return builder
