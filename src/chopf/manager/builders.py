"""Import-time collectors for declarative specs.

Builders are the user-facing decorator API. They exist only to collect
frozen specs (see chopf.specs); they never reference runtime instances.
The manager materializes specs into running components.
"""
import logging

from ..builder import resource_rbac
from ..controller import requests_from_event_for_owner
from ..specs import ControllerSpec, InformerSpec, WatchSpec

log = logging.getLogger(__name__)


def _parens_optional(register, func=None):
    """Support both @decorator and @decorator() forms."""

    def decorator(f):
        register(f)

    if func is None:
        # Called as @decorator() with parens.
        return decorator
    # Called as @decorator without parens.
    return decorator(func)


class ControllerBuilder:
    """Collects a ControllerSpec via decorators at import time."""

    def __init__(self, manager, resource, name=None) -> None:
        self.manager = manager
        self.resource = resource
        self.name = name
        self._reconcile = None
        self._concurrent_reconciles = 1
        self._predicates = []
        self._watches = []
        self._tasks = []
        self._startup = None
        self._shutdown = None
        # Register minimal RBAC for this controller to work with his
        # primary resource.
        resource_rbac(resource, verbs='get;list;watch')

    def to_spec(self) -> ControllerSpec:
        """Return the frozen spec collected so far."""
        return ControllerSpec(
            resource=self.resource,
            name=self.name,
            reconcile=self._reconcile,
            predicates=tuple(self._predicates),
            watches=tuple(self._watches),
            tasks=tuple(self._tasks),
            startup=self._startup,
            shutdown=self._shutdown,
            concurrent_reconciles=self._concurrent_reconciles,
        )

    @property
    def store(self):
        return self.manager.store(self.resource)

    def get_index(self, index_name):
        store = self.manager.store(self.resource)
        return store.get_index(
            index_name,
            resource=self.resource,
        )

    def _add_watch(self, resource, handler, **kwargs):
        self.manager.register_resource(resource)
        self._watches.append(WatchSpec(resource, handler, kwargs))

    def watch_owner(self, resource):
        """Function that registers a watch for the given resource
        and that enqueues the owning resource if it is of the same api_version
        and type as the resources managed by this controller.
        """
        # Register minimal RBAC for watch to work.
        resource_rbac(resource, verbs='get;list;watch')
        self._add_watch(
            resource, requests_from_event_for_owner, owner=self.resource
        )

    def watch(self, resource):
        """Decorator that registers a watch for the given resource.
        The decorated function must yield Request instances that
        are then added to the workqueue for reconcilation.
        """
        # Register minimal RBAC for watch to work.
        resource_rbac(resource, verbs='get;list;watch')

        def decorator(f):
            self._add_watch(resource, f)

        return decorator

    def predicate(self, func=None, /):
        """Decorator that registers a predicate function with this controller.
        All registered predicates must return True for a request to be added
        to the workqueue for reconcilation.
        """
        return _parens_optional(self._predicates.append, func)

    def index(self, index_name):
        """Decorator that registers an indexer with this controllers store."""
        store = self.manager.store(self.resource)
        return store.index(index_name)

    def startup(self, func=None, /):
        """Decorator that registers an startup function with this controller."""
        if callable(self._startup):
            raise Exception(
                f'Controller already has a startup function registered: {self._startup}'
            )
        return _parens_optional(self._set_startup, func)

    def _set_startup(self, f):
        self._startup = f

    def shutdown(self, func=None, /):
        """Decorator that registers an shutdown function with this controller."""
        if callable(self._shutdown):
            raise Exception(
                f'Controller already has a shutdown function registered: {self._shutdown}'
            )
        return _parens_optional(self._set_shutdown, func)

    def _set_shutdown(self, f):
        self._shutdown = f

    def task(self, func=None, /):
        """Decorator that registers a background task function with this controller.
        Async tasks are run in the main loop while sync tasks are run in a thread pool.
        """
        return _parens_optional(self._tasks.append, func)

    def reconcile(self, func=None, /, *, concurrency=1):
        """Decorator that registers a reconcile function with this controller."""
        if callable(self._reconcile):
            raise Exception(
                f'Controller already has a reconcile function registered: {self._reconcile}'
            )
        self._concurrent_reconciles = concurrency
        return _parens_optional(self._set_reconcile, func)

    def _set_reconcile(self, f):
        self._reconcile = f


class InformerBuilder:
    """Collects an InformerSpec via decorators at import time."""

    def __init__(
        self,
        manager,
        resource,
        name=None,
        resync_after=None,
        timeout=10,
    ) -> None:
        self.manager = manager
        self.resource = resource
        self.name = name
        self.resync_after = resync_after
        self.timeout = timeout
        self._transformer = None
        self._stream_receivers = []

    def to_spec(self) -> InformerSpec:
        """Return the frozen spec collected so far."""
        return InformerSpec(
            resource=self.resource,
            name=self.name,
            resync_after=self.resync_after,
            timeout=self.timeout,
            transformer=self._transformer,
            stream_receivers=tuple(self._stream_receivers),
        )

    @property
    def store(self):
        return self.manager.store(self.resource)

    def get_index(self, index_name):
        store = self.manager.store(self.resource)
        return store.get_index(
            index_name,
            resource=self.resource,
        )

    def stream(self, func=None, /):
        """Decorator that registers a stream receiver with this informer."""
        return _parens_optional(self._stream_receivers.append, func)

    def index(self, index_name):
        """Decorator that registers an indexer function with this informer."""
        store = self.manager.store(self.resource)
        return store.index(index_name)

    def transform(self, func=None, /):
        """Decorator that registers a transformer function with this informer."""
        return _parens_optional(self._set_transformer, func)

    def _set_transformer(self, f):
        self._transformer = f
