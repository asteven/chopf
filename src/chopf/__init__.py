# All types a user would care about are made available in the top level package.
# A user should never have to import anything from sub modules.

# ruff: noqa: I001 - import order matters here to avoid circular imports
from .exceptions import *
from .resources import *
from .cache import *
from .source import *
from .controller import *
from .client import *
from .observer import *
from .builder import *
from .manager import Manager

# Singleton manager instance.
manager = Manager()

# Easy access to start manager.
run = manager.run

# Easy access to builder decorators.
controller = manager.controller
informer = manager.informer
store = manager.store


def get_store(resource):
    return manager.get_store(resource)


def get_index(resource, index_name):
    store = manager.get_store(resource)
    return store.get_index(
        index_name,
        resource=resource,
    )


def index(resource, index_name):
    """Decorator that registers an indexer function with the given resources store."""
    # Register minimal RBAC so the informer which will be populating this store
    # can do its job.
    resource_rbac(resource, verbs='get;list;watch')
    store = manager.store(resource)
    return store.index(index_name)


def get_async_client():
    return manager.async_client


def get_sync_client():
    return manager.sync_client
