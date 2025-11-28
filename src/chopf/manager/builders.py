import functools
import collections
import logging

from ..controller import requests_from_event_for_owner
from ..builder import resource_rbac


log = logging.getLogger(__name__)


class Builder:
    """A Builder is used to collect information at import time
    that is later used to create actual instances at runtime.
    """


class StoreBuilder(Builder):
    def __init__(self, manager, resource) -> None:
        self.manager = manager
        self.resource = resource
        self._kwargs = {
            'indexers': {},
        }
        self._instance = None

    def __getattr__(self, key):
        # proxy to Store instance
        return getattr(self._instance, key)

    def __iter__(self):
        return iter(self._instance)

    def index(self, index_name):
        """Decorator that registers an indexer function with the given name."""

        def decorator(f):
            self._kwargs['indexers'].update({index_name: f})

        return decorator


class ControllerBuilder(Builder):
    def __init__(self, manager, resource, name=None) -> None:
        self.manager = manager
        self.resource = resource
        self._kwargs = {
            'name': name,
            'predicates': [],
            'tasks': [],
            'watches': {},
            'startup': None,
            'shutdown': None,
            'reconcile': None,
            'concurrent_reconciles': 1,
        }
        self._instance = None
        # Register minimal RBAC for this controller to work with his
        # primary resource.
        resource_rbac(resource, verbs='get;list;watch')

    def __getattr__(self, key):
        # proxy to Controller instance
        return getattr(self._instance, key)

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
        self._kwargs['watches'][resource] = {
            'resource': resource,
            'handler': handler,
            'kwargs': kwargs,
        }

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

        def decorator(f):
            self._kwargs['predicates'].append(f)

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)

    def index(self, index_name):
        """Decorator that registers an indexer with this controllers store."""
        store = self.manager.store(self.resource)
        return store.index(index_name)

    def startup(self, func=None, /):
        """Decorator that registers an startup function with this controller."""
        existing = self._kwargs.get('startup', None)
        if callable(existing):
            raise Exception(
                f'Controller already has a startup function registered: {existing}'
            )

        def decorator(f):
            self._kwargs['startup'] = f

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)

    def shutdown(self, func=None, /):
        """Decorator that registers an shutdown function with this controller."""
        existing = self._kwargs.get('shutdown', None)
        if callable(existing):
            raise Exception(
                f'Controller already has a shutdown function registered: {existing}'
            )

        def decorator(f):
            self._kwargs['shutdown'] = f

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)

    def task(self, func=None, /):
        """Decorator that registers a background task function with this controller.
        Async tasks are run in the main loop while sync tasks are run in a thread pool.
        """

        def decorator(f):
            self._kwargs['tasks'].append(f)

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)

    def reconcile(self, func=None, /, *, concurrency=1):
        """Decorator that registers a reconcile function with this controller."""
        existing = self._kwargs.get('reconcile', None)
        if callable(existing):
            raise Exception(
                f'Controller already has a reconcile function registered: {existing}'
            )
        self._kwargs['concurrent_reconciles'] = concurrency

        def decorator(f):
            self._kwargs['reconcile'] = f

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)


class InformerBuilder(Builder):
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
        self._kwargs = {
            'name': name,
            'resync_after': resync_after,
            'timeout': timeout,
            'transformer': None,
        }
        self._instance = None
        self._stream_receivers = []

    #def __getattr__(self, key):
    #    # proxy to Informer instance
    #    return getattr(self._instance, key)

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

        def decorator(f):
            self._stream_receivers.append(f)

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)

    def index(self, index_name):
        """Decorator that registers an indexer function with this informer."""
        store = self.manager.store(self.resource)
        return store.index(index_name)

    def transform(self, func=None, /):
        """Decorator that registers a transformer function with this informer."""

        def decorator(f):
            self._kwargs['transformer'] = f

        if func is None:
            # We're called as @decorator() with parens.
            return decorator
        else:
            # We're called as @decorator without parens.
            return decorator(func)
