import importlib

import pytest
from lightkube.resources.core_v1 import Pod

manager_mod = importlib.import_module('chopf.manager.manager')


class DummyCache:
    def __init__(self, api_client):
        self.api_client = api_client
        self._all_namespaces = False
        self.namespaces = set()
        self.resources = set()
        self._stores = {}

    def get_store(self, resource):
        return self._stores.setdefault(resource, object())

    def reconcile_informers(self):
        pass


class DummyClient:
    namespace = 'default'

    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture(autouse=True)
def patch_manager_dependencies(monkeypatch):
    monkeypatch.setattr(manager_mod, 'LightkubeAsyncClient', DummyClient)
    monkeypatch.setattr(manager_mod, 'LightkubeSyncClient', DummyClient)
    monkeypatch.setattr(manager_mod, 'Cache', DummyCache)


@pytest.fixture
def manager():
    return manager_mod.Manager()


def test_register_resource_calls_rbac(monkeypatch, manager):
    calls = []

    def fake_rbac(resource, verbs):
        calls.append((resource, verbs))

    monkeypatch.setattr(manager_mod, 'resource_rbac', fake_rbac)

    manager.register_resource(Pod)

    assert Pod in manager.resources
    assert calls == [(Pod, 'get;list;watch')]


def test_store_returns_cache_store(monkeypatch, manager):
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *args, **kwargs: None)

    store = manager.store(Pod)

    assert store is manager.cache.get_store(Pod)
    assert Pod in manager.resources


def test_controller_builder_cached(monkeypatch, manager):
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *args, **kwargs: None)

    builder_a = manager.controller(Pod)
    builder_b = manager.controller(Pod)

    assert builder_a is builder_b


def test_informer_builder_cached(monkeypatch, manager):
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *args, **kwargs: None)

    builder_a = manager.informer(Pod)
    builder_b = manager.informer(Pod)

    assert builder_a is builder_b


def test_configure_cache_updates_cache(monkeypatch, manager):
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *args, **kwargs: None)
    manager._active_namespaces = {'ns1'}
    manager._active_resources = {Pod}
    manager.all_namespaces = False

    manager.configure_cache()

    assert manager.cache._all_namespaces is False
    assert manager.cache.namespaces == {'ns1'}
    assert manager.cache.resources == {Pod}


@pytest.mark.anyio
async def test_add_namespace_triggers_reconcile(monkeypatch, manager):
    calls = []

    async def fake_reconcile(startup=False):
        calls.append(startup)

    manager.reconcile = fake_reconcile

    await manager.add_namespace('default')

    assert 'default' in manager._active_namespaces
    assert calls == [False]


@pytest.mark.anyio
async def test_remove_namespace_triggers_reconcile(monkeypatch, manager):
    manager._active_namespaces = {'default'}
    calls = []

    async def fake_reconcile(startup=False):
        calls.append(startup)

    manager.reconcile = fake_reconcile

    await manager.remove_namespace('default')

    assert 'default' not in manager._active_namespaces
    assert calls == [False]


@pytest.mark.anyio
async def test_add_resource_triggers_reconcile(monkeypatch, manager):
    calls = []

    async def fake_reconcile(startup=False):
        calls.append(startup)

    manager.reconcile = fake_reconcile

    await manager.add_resource(resource=Pod)

    assert Pod in manager._active_resources
    assert calls == [False]


@pytest.mark.anyio
async def test_remove_resource_triggers_reconcile(monkeypatch, manager):
    manager._active_resources = {Pod}
    calls = []

    async def fake_reconcile(startup=False):
        calls.append(startup)

    manager.reconcile = fake_reconcile

    await manager.remove_resource(resource=Pod)

    assert Pod not in manager._active_resources
    assert calls == [False]
