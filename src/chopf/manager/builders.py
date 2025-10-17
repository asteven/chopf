import functools
import collections
import logging

from ..controller import requests_from_event_for_owner


log = logging.getLogger(__name__)


Callback = collections.namedtuple('Callback', ['func', 'args', 'kwargs'])


class CallbackCollector:
    def __init__(self, events):
        self._events = events
        self._callbacks = {}

    def __repr__(self):
        _id = id(self)
        return f'<{self.__class__.__name__} {_id} {self._events} {self._callbacks}>'

    def __getattr__(self, key):
        if key in self._events:
            callbacks = self._callbacks.get(key, None)
            if callbacks is None:
                callbacks = []
                self._callbacks[key] = callbacks

            def decorator(*args, **kwargs):
                if len(args) == 1 and callable(args[0]):
                    # Used as:
                    # @on.something
                    # def some_function(*args, **kwargs):
                    #     pass
                    func = args[0]
                    d_args = kwargs.get('__args', [])
                    d_kwargs = kwargs.get('__kwargs', {})
                    callbacks.append(Callback(func, d_args, d_kwargs))
                else:
                    # Used as:
                    # @on.something(arg1, arg2, kw1=1, kw2=2)
                    # def some_function(*args, **kwargs):
                    #     pass
                    return functools.partial(decorator, __args=args, __kwargs=kwargs)

            return decorator
        else:
            raise AttributeError(key)

    def __getitem__(self, key):
        # return the callbacks for the given key
        return self._callbacks[key]

    def items(self):
        return self._callbacks.items()


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
            'watches': {},
            'startup': None,
            'shutdown': None,
            'reconcile': None,
            'concurrent_reconciles': 1,
        }
        self._instance = None

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

    def _add_watch(self, resource, handler, meta=False, **kwargs):
        # TODO: pass `meta` to watcher so it can set header on request like:
        #    params = {
        #        "header_params": {
        #            "Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"
        #        }
        #    }
        self.manager.register_resource(resource)
        self._kwargs['watches'][resource] = {
            'resource': resource,
            'handler': handler,
            'meta': meta,
            'kwargs': kwargs,
        }

    def watch_owner(self, resource, meta=False):
        """Function that registers a watch for the given resource
        and that enqueues the owning resource if it is of the same api_version
        and type as the resources managed by this controller.
        """
        # TODO: generate rbac for: get, list, watch
        self._add_watch(
            resource, requests_from_event_for_owner, meta=meta, owner=self.resource
        )

    def watch(self, resource, meta=False):
        """Decorator that registers a watch for the given resource.
        The decorated function must yield Request instances that
        are then added to the workqueue for reconcilation.
        """

        # TODO: generate rbac for: get, list, watch
        def decorator(f):
            self._add_watch(resource, f, meta=meta)

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

    def reconcile(self, func=None, /, *, concurrency=1):
        """Decorator that registers an reconcile function with this controller."""
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
