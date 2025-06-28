# All types a user would care about are made available in the top level package.
# A user should never have to import anything from sub modules.

from .exceptions import *  # noqa: F403 public API
from .resources import *  # noqa: F403 public API
from .cache import *  # noqa: F403 public API
from .source import *  # noqa: F403 public API
from .controller import *  # noqa: F403 public API
from .client import *  # noqa: F403 public API
from .manager import Manager

# Singleton manager instance.
manager = Manager()

# Easy access to start manager.
run = manager.run

# Easy access to builder decorators.
controller = manager.controller
informer = manager.informer
store = manager.store


def get_store(api_version, kind):
    return manager.cache.get_store(api_version, kind)


def get_async_client():
    return manager.async_client


def get_sync_client():
    return manager.sync_client
