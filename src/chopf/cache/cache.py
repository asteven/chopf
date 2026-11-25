import copy
import logging

import anyio
from anyio import TASK_STATUS_IGNORED
from anyio.abc import TaskStatus

from ..exceptions import Error, ObjectNotFound
from ..membership import InformerRef, plan_informers, ref_for
from ..observer import Observer
from ..resources import PartialObjectMetadata, identity
from ..tasks import Task
from . import Indexer, Informer

log = logging.getLogger(__name__)


# - one store for each gvk
# - when watching specific namespaces:
#   - one informer for each combination of gvk and namespace, all backed by the shared gvk store
#   - one controller for each gvk using the shared gvk store
# - when watching all namespaces:
#   - one informer per gvk using the same gvk store
#   - one controller for each gvk using the shared gvk store
#

ALL_NAMESPACES = {'*'}


class Cache(Task):
    # def __init__(self, api_client, all_namespaces=False,
    #        namespaces=None, resources=None):
    def __init__(self, api_client, stores=None):
        super().__init__()
        self.api_client = api_client
        # self._all_namespaces = all_namespaces
        self._all_namespaces = False
        # self._namespaces = namespaces or set()
        self._namespaces = set()
        # self._resources = resources or set()
        self._resources = set()

        self._task_group = None  # Main taskgroup
        self._stop = anyio.Event()
        self._synced = anyio.Event()
        self._registered_resources = set()
        # Store registry; the manager's import-time registry is adopted
        # when given (indexers registered before the cache exists).
        self._stores = stores if stores is not None else {}
        self._informers = []
        # Instrumentation hooks, propagated to informers created here.
        self.observer = Observer()

    def __repr__(self):
        _id = id(self)
        resources = {identity.gvk(r) for r in self._resources}
        return f'<Cache {_id} namespaces: {self.namespaces} resources: {resources}>'

    @property
    def synced(self):
        return self._synced.wait()

    @property
    def resources(self):
        return self._resources

    @resources.setter
    def resources(self, value):
        log.debug(f'resources: {self.resources} -> {value}')
        if value is not None:
            removed_resources = self.resources.difference(value)
            self._purge_stores(resources=removed_resources)
            self._resources = value

    @property
    def namespaces(self):
        if self._all_namespaces:
            return ALL_NAMESPACES
        else:
            return self._namespaces

    @namespaces.setter
    def namespaces(self, value):
        if value:
            removed_namespaces = self._namespaces.difference(value)
            self._purge_stores(namespaces=removed_namespaces)
            self._namespaces = value

    def _purge_stores(self, namespaces=None, resources=None):
        if resources:
            # Clear the stores for the given resources.
            for resource in resources:
                store = self._stores[resource]
                store.clear()
        if namespaces:
            for namespace in namespaces:
                # Purge all namespaced objects from all stores.
                for store in self._stores.values():
                    try:
                        index_by_namespace = store.get_index('namespace')
                        for obj in index_by_namespace[namespace]:
                            store.delete(obj)
                    except KeyError:
                        pass

    def reconcile_informers(self):
        self._synced = anyio.Event()
        resource_classes = {ref_for(resource): resource for resource in self.resources}
        if self._all_namespaces:
            namespaces = frozenset()
        else:
            namespaces = frozenset(self._namespaces)
        current = frozenset(
            InformerRef(ref_for(informer.resource), informer.namespace)
            for informer in self._informers
        )
        plan = plan_informers(
            frozenset(resource_classes),
            namespaces,
            current,
            all_namespaces=self._all_namespaces,
        )

        # Remove obsolete informers.
        for informer in self._informers.copy():
            ref = InformerRef(ref_for(informer.resource), informer.namespace)
            if ref in plan.stop:
                self.remove_informer(informer)

        # Create required informers. They are started in reconcile().
        for ref in plan.start:
            resource = resource_classes[ref.resource]
            self.get_informer(resource, namespace=ref.namespace)

    async def reconcile(self):
        if self._task_group is None:
            # The cache is not running: informers cannot start, so we
            # must not report the cache as synced. Controllers would
            # reconcile against an empty store.
            log.warning('reconcile called on a cache that is not running')
            return
        # Ensure all informers are running. Note that task_group.start()
        # returns after the informer's initial list has completed, so
        # setting synced here means the stores are actually populated.
        for informer in self._informers:
            if not informer.is_running:
                await self._task_group.start(informer)
        self._synced.set()

    def add_informer(self, informer):
        self._informers.append(informer)

    def remove_informer(self, informer):
        # Stop the informer.
        informer.stop()
        # Forget about the informer.
        self._informers.remove(informer)

    def register_resource(self, resource):
        self._registered_resources.add(resource)
        if resource not in self._resources:
            # Should never happen.
            raise Error(f'resource registered from unknown source: {resource}')
        self._resources.add(resource)

    def get_store(self, resource):
        # self.register_resource(resource)
        try:
            store = self._stores[resource]
        except KeyError:
            store = self._stores[resource] = Indexer()
        return store

    def get_informers_by(self, resource=None, namespace=None):
        # log.debug(f'get_informers_by: {resource} {namespace}')
        if self._all_namespaces and identity.is_namespaced(resource):
            namespace = '*'
        informers = [
            informer
            for informer in self._informers
            if all(
                (
                    (namespace is None or informer.namespace == namespace),
                    (resource is None or informer.resource == resource),
                )
            )
        ]
        # log.debug(f'get_informers_by: informers: {informers}')
        return informers

    def get_informer(self, resource, namespace=None, create=True):
        # log.debug(f'get_informer: {resource} {namespace}')
        if self._all_namespaces and identity.is_namespaced(resource):
            namespace = '*'
        informer = None
        try:
            informer = self.get_informers_by(
                resource=resource,
                namespace=namespace,
            )[0]
            log.debug(f'get_informer: informer: {informer}')
        except IndexError:
            if create:
                informer = Informer(
                    self.api_client,
                    self.get_store(resource),
                    resource,
                    namespace=namespace,
                    observer=self.observer,
                )
                self.add_informer(informer)
        return informer

    def stop(self):
        log.debug('stop %r', self)
        if self._task_group:
            self._task_group.cancel_scope.cancel()

    async def __call__(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        log.debug('starting %r', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    log.info('started %s', self)

                    # Inform any awaiters that we are ready.
                    self._running.set()
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
