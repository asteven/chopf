import anyio
import copy
import logging

from anyio.abc import CancelScope

from ..tasks import Task
from ..resources import PartialObjectMetadata
from ..exceptions import ObjectNotFound
from . import Informer, Indexer


log = logging.getLogger(__name__)


# - one store for each gvk
# - when watching specific namespaces:
#   - one informer for each combination of gvk and namespace, all backed by the shared gvk store
#   - one controller for each gvk using the shared gvk store
# - when watching all namespaces:
#   - one informer per gvk using the same gvk store
#   - one controller for each gvk using the shared gvk store
#

ALL_NAMESPACES = ['*']


class Cache(Task):
    def __init__(self, api_client, all_namespaces=False, namespaces=None):
        super().__init__()
        self.api_client = api_client
        self._all_namespaces = all_namespaces
        self._namespaces = namespaces or []

        self._task_group = None  # Main taskgroup
        self._stop = anyio.Event()
        self._resources = set()
        self._stores = {}
        self._event_sources = {}
        self._informers = []

    def __repr__(self):
        _id = id(self)
        resources = [f'{r.apiVersion}/{r.kind}' for r in self._resources]
        return f'<Cache {_id} namespaces: {self.namespaces} resources: {resources}>'

    def is_watched_resource(self, resource, namespace):
        informer = self.get_informer(resource, namespace=namespace, create=False)
        return informer is not None

    @property
    def namespaces(self):
        if self._all_namespaces:
            return ALL_NAMESPACES
        else:
            return self._namespaces

    async def add_namespace(self, namespace):
        log.debug('adding namespace: %s', namespace)
        if namespace not in self._namespaces:
            self._namespaces.append(namespace)
            await self.start_namespace(namespace)

    async def start_namespace(self, namespace):
        log.debug('starting namespace: %s', namespace)
        for resource in self._resources:
            # Get an informer for this resource.
            informer = self.get_informer(resource, namespace=namespace)
            # Wait until the informer has started.
            await informer

    async def remove_namespace(self, namespace):
        log.debug('removing namespace: %s', namespace)
        if namespace in self._namespaces:
            self._namespaces.remove(namespace)
            await self.stop_namespace(namespace)

    async def stop_namespace(self, namespace):
        log.debug('stopping namespace: %s', namespace)
        for informer in self.get_informers_by(namespace=namespace):
            self.remove_informer(informer)

        # Purge all namespaced objects from all stores.
        for store in self._stores.values():
            try:
                index_by_namespace = store.get_index('namespace')
                for obj in index_by_namespace[namespace]:
                    store.delete(obj)
            except KeyError:
                pass

    def add_informer(self, informer):
        try:
            # Add the informer to all revelant event sources.
            sources = self._event_sources[informer.resource]
            for source in sources:
                source.add_informer(informer)
        except KeyError:
            pass
        finally:
            # Remember the informer.
            self._informers.append(informer)
            # Ensure the informer will be started.
            if not informer.is_running:
                self._task_group.start_soon(informer)

    def remove_informer(self, informer):
        # Stop the informer.
        informer.stop()
        try:
            # Remove the informer from all event sources.
            sources = self._event_sources[informer.resource]
            for source in sources:
                source.remove_informer(informer)
        except KeyError:
            pass
        finally:
            # Finally forget about the informer.
            self._informers.remove(informer)

    def add_event_source(self, source):
        self.register_resource(source.resource)
        try:
            sources = self._event_sources[source.resource]
        except KeyError:
            sources = self._event_sources[source.resource] = []
        finally:
            sources.append(source)

    def remove_event_source(self, source):
        try:
            self._event_sources[source.resource].remove(source)
        except (KeyError, ValueError) as e:
            log.debug('remove_event_source failed: %r', e)
            pass

    def register_resource(self, resource):
        self._resources.add(resource)

    def get_store(self, resource):
        self.register_resource(resource)
        try:
            store = self._stores[resource]
        except KeyError:
            store = self._stores[resource] = Indexer()
        finally:
            return store

    def get_informers_by(self, resource=None, namespace=None):
        if self._all_namespaces:
            namespace = '*'
        return [
            informer
            for informer in self._informers
            if all(
                (
                    (namespace is None or informer.namespace == namespace),
                    (resource is None or informer.resource == resource),
                )
            )
        ]

    def get_informer(self, resource, namespace=None, create=True):
        if self._all_namespaces:
            namespace = '*'
        informer = None
        try:
            informer = self.get_informers_by(
                resource=resource,
                namespace=namespace,
            )[0]
        except IndexError:
            if create:
                informer = Informer(
                    self.api_client,
                    self.get_store(resource),
                    resource,
                    namespace=namespace,
                )
                self.add_informer(informer)
        return informer

    def stop(self):
        log.debug('stop %r', self)
        if self._task_group:
            self._task_group.cancel_scope.cancel()

    async def __call__(self):
        log.debug('starting %r', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    for namespace in self.namespaces:
                        await self.start_namespace(namespace)

                    # We are running and all our caches are synced.
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
                    with CancelScope(shield=True):
                        for namespace in self.namespaces:
                            await self.stop_namespace(namespace)

        finally:
            log.debug('stopped %s', self)

    async def get(self, resource, namespace=None, name=None):
        """Get from cache"""
        # The store contains all objects of all watched namespaces.
        store = self.get_store(resource)
        # As the store is namespace aware we can just pass any given namespace
        # down to the store to get the proper object.
        key_obj = PartialObjectMetadata(name, namespace=namespace)
        try:
            obj = store.get(key_obj)
            # We return a copy, so that external changes don't change the
            # original in the store.
            return copy.deepcopy(obj)
        except KeyError as e:
            raise ObjectNotFound(key_obj) from e

    async def list(self, resource, namespace=None):
        """List from cache."""
        # The store contains all objects of all watched namespaces.
        # If a list of objects in a specific namespace is requested
        # use the builtin 'namespace' index to return it.
        store = self.get_store(resource)
        if namespace is not None:
            index = store.get_index('namespace')
            # We return copies, so that external changes don't change the
            # object in the store.
            return [copy.deepcopy(obj) for obj in index[namespace]]
        else:
            # We return copies, so that external changes don't change the
            # object in the store.
            return [copy.deepcopy(obj) for obj in store.list()]
