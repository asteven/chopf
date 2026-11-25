import functools
import logging
import platform
import signal
import uuid

import anyio
import uvloop
from anyio import TASK_STATUS_IGNORED, CancelScope, open_signal_receiver
from anyio.abc import TaskStatus
from lightkube import AsyncClient as LightkubeAsyncClient
from lightkube.core.resource_registry import LoadResourceError, resource_registry

from .. import exceptions
from ..builder import resource_rbac
from ..cache import Cache, Indexer
from ..client import AsyncClient, SyncClient
from ..controller import Controller
from ..observer import Observer
from ..resources import custom_resource_registry, identity
from ..tasks import Task
from .builders import ControllerBuilder, InformerBuilder

# Re-enable together with the LeaseController startup below.
from .controllers import LeaseController  # noqa: F401
from .system import InternalRuntime

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
        return hash(
            (
                identity.api_version(self.resource),
                identity.kind(self.resource),
                self.handler,
            )
        )

    def __repr__(self):
        return f'<StreamReceiver {identity.gvk(self.resource)} {self.handler}>'

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
        # Instrumentation seam; assign your own Observer before run().
        self.observer = Observer()
        # Runtime components (lightkube client, cache, client facades,
        # internal runtime) are created when the operator actually
        # starts — never at import: importing chopf (e.g. to generate
        # CRD/RBAC manifests) must not require a kubeconfig, and nothing
        # at import time may hold references to runtime components.
        self.async_api_client = None
        self.cache = None
        self.async_client = None
        self.sync_client = None
        self.internal_runtime = None
        # The store registry is pure data (resource -> Indexer): usable
        # at import time for index registration, adopted by the Cache
        # when it is created at runtime.
        self._stores = {}
        self.is_active = False
        # The unique ID of this operator, used in leader election.
        self.operator_id = (
            f'{platform.node()}-{str(uuid.uuid4()).replace("-", "")[:10]}'
        )
        self._builders = {
            'controller': {},
            'informer': {},
        }
        # The namespace this manager runs in (set in _setup_runtime).
        self.manager_namespace = None
        self._active_namespaces = set()
        self._active_resources = set()
        self._controllers = {}
        self._stream_receivers = {}
        # Custom resource registry; defaults to the process-wide registry
        # that import-time decorators register into, injectable for tests.
        self.custom_resources = custom_resource_registry
        self._main_task_group = None  # Main taskgroup
        self._task_group = None  # regular taskgroup
        self._stop = None
        self._exit = anyio.Event()

    def __repr__(self):
        _id = id(self)
        # namespaces = self._active_namespaces
        namespaces = self.namespaces
        resources = {identity.gvk(resource) for resource in self.resources}
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
            await self.sync_membership()

    async def remove_namespace(self, namespace):
        if namespace in self._active_namespaces:
            log.info('namespace_removed: %s', namespace)
            self._active_namespaces.remove(namespace)
            await self.sync_membership()

    async def add_resource(self, resource=None, version=None, kind=None):
        if resource is None:
            resource = resource_registry.load(version, kind)
        if not resource in self._active_resources:
            log.info('resource_added: %s', identity.gvk(resource))
            self._active_resources.add(resource)
            await self.sync_membership()

    async def remove_resource(self, resource=None, version=None, kind=None):
        if resource is None:
            resource = resource_registry.load(version, kind)
        if resource in self._active_resources:
            log.info('resource_removed: %s', identity.gvk(resource))
            self._active_resources.remove(resource)
            await self.sync_membership()

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

    async def sync_membership(self, startup=False):
        log.debug(f'sync_membership: is_active: {self.is_active},  startup={startup}')
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
                        # self._task_group.start_soon(stream_receiver)
                for informer in self.cache.get_informers_by(resource=resource):
                    for stream_receiver in stream_receivers:
                        if not informer.has_stream(key=stream_receiver):
                            informer.add_stream(
                                stream_receiver.stream, key=stream_receiver
                            )
            else:
                for stream_receiver in stream_receivers:
                    stream_receiver.stop()

        # Start or stop controllers and connect them to their informers.
        for resource, controller in self._controllers.items():
            if resource in self._active_resources:
                if not controller.is_running:
                    await self._task_group.start(controller)
                #    #self._task_group.start_soon(controller)

                # for source in controller.event_sources:
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
                resource_registry.load(
                    identity.api_version(resource), identity.kind(resource)
                )
                await self.add_resource(resource)
            except LoadResourceError:
                log.warning(
                    'ignoring unusable resource: %s',
                    identity.gvk(resource),
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

                    # Start the cache and wait until it is running, so
                    # sync_membership can rely on its task group existing.
                    await tg.start(self.cache)

                    # Initial reconcilation.
                    tg.start_soon(self.sync_membership, True)

                    # log.debug('started %s', self)
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

    def _setup_runtime(self):
        """Create the runtime components: lightkube client, cache and
        client facades. Called when the operator actually starts
        (Manager.run/__call__), never at import — importing chopf must
        not require a kubeconfig."""
        self.async_api_client = LightkubeAsyncClient()
        # The cache adopts the store registry that was populated at
        # import time via store()/get_store().
        self.cache = Cache(self.async_api_client, stores=self._stores)
        self.cache.observer = self.observer
        self.async_client = AsyncClient(
            self.async_api_client, self.cache, observer=self.observer
        )
        self.sync_client = SyncClient(self.async_client)
        self.manager_namespace = self.async_api_client.namespace

    async def __call__(
        self, all_namespaces=False, namespaces=None, setup_signal_handler=False
    ):
        self._setup_runtime()
        self.all_namespaces = all_namespaces
        if not self.all_namespaces:
            if namespaces is not None:
                self.namespaces = set(namespaces)
            else:
                self.namespaces = {self.async_api_client.namespace}

        log.debug('startup %s', self)

        for builder in self._builders['informer'].values():
            log.debug('creating stream receivers from informer builder: %r', builder)
            spec = builder.to_spec()
            for receiver in spec.stream_receivers:
                stream_receiver = StreamReceiver(spec.resource, receiver)
                self.add_stream_receiver(stream_receiver)

        for builder in self._builders['controller'].values():
            log.debug('creating controller from builder: %r', builder)
            self._materialize_controller(builder.to_spec())

        self.internal_runtime = InternalRuntime(self)
        self.internal_runtime._setup_controllers()

        try:
            async with anyio.create_task_group() as tg:
                self._main_task_group = tg
                if setup_signal_handler:
                    tg.start_soon(signal_handler, tg.cancel_scope)

                # Internal controllers first: they run with their own
                # cache (no dependency on the shared one) and must be
                # synced before the payload starts. In an HA setup a
                # follower runs only this part — no shared cache, no
                # user controllers.
                await self.internal_runtime.start(tg)

                tg.start_soon(self.start)

                # Wait until told otherwise.
                await self._exit.wait()
        except* exceptions.Error as eg:
            if self.debug:
                raise
            error_messages = []
            for error in exceptions.iterate_errors(eg):
                error_messages.append(str(error))
            raise exceptions.FatalError(' '.join(error_messages)) from eg

        log.debug('exiting %s', self)

    def _materialize_controller(self, spec):
        """Construct a controller from a spec — one path for both the
        builder front-end (plain callables) and the subclass front-end
        (controller_class set, manager passed explicitly)."""
        if spec.controller_class is not None:
            controller = spec.controller_class(
                self,
                self.async_client,
                self.sync_client,
                self.cache,
                spec.resource,
                name=spec.name,
                predicates=list(spec.predicates),
                watches=list(spec.watches),
                tasks=list(spec.tasks),
                concurrent_reconciles=spec.concurrent_reconciles,
                wait_for_cache=spec.wait_for_cache,
                observer=self.observer,
            )
        else:
            controller = Controller(
                self.async_client,
                self.sync_client,
                self.cache,
                spec.resource,
                name=spec.name,
                predicates=list(spec.predicates),
                watches=list(spec.watches),
                tasks=list(spec.tasks),
                reconcile=spec.reconcile,
                startup=spec.startup,
                shutdown=spec.shutdown,
                concurrent_reconciles=spec.concurrent_reconciles,
                wait_for_cache=spec.wait_for_cache,
                observer=self.observer,
            )
        self._controllers[spec.resource] = controller
        return controller

    def register_resource(self, resource):
        # Register minimal read-only RBAC so the informers and clients which
        # will be working with the resource can do their job.
        resource_rbac(resource, verbs='get;list;watch')
        self.resources.add(resource)

    def get_store(self, resource):
        """The store for a resource, from the manager's registry.

        Works at import time (no cache needed): the registry is pure
        data and is adopted by the Cache when it is created at runtime.
        """
        try:
            return self._stores[resource]
        except KeyError:
            return self._stores.setdefault(resource, Indexer())

    def store(self, resource):
        """Register the resource and return its live store.

        Stores are created eagerly at registration time, so indexers can
        be registered directly on the returned store via its index
        decorator.
        """
        self.register_resource(resource)
        return self.get_store(resource)

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
