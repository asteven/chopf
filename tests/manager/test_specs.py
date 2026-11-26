"""Characterization tests for the builder -> spec -> materialization pipeline.

Seam under test: the decorator API used at import time
(``chopf.controller()``, ``chopf.informer()``, ``chopf.store()``) and the
frozen spec objects the manager materializes runtime components from.
"""

import dataclasses
import importlib

import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

manager_mod = importlib.import_module('chopf.manager.manager')
builders_mod = importlib.import_module('chopf.manager.builders')

from chopf.cache.index import Indexer
from chopf.specs import ControllerSpec, InformerSpec


class DummyCache:
    def __init__(self, api_client):
        self.api_client = api_client
        self._all_namespaces = False
        self.namespaces = set()
        self.resources = set()
        self._stores = {}

    def get_store(self, resource):
        return self._stores.setdefault(resource, Indexer())


class DummyClient:
    namespace = 'default'

    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture(autouse=True)
def patch_manager_dependencies(monkeypatch):
    monkeypatch.setattr(manager_mod, 'LightkubeAsyncClient', DummyClient)
    monkeypatch.setattr(manager_mod, 'Cache', DummyCache)
    monkeypatch.setattr(builders_mod, 'resource_rbac', lambda *a, **kw: None)
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *a, **kw: None)


@pytest.fixture
def manager():
    return manager_mod.Manager()


def test_controller_spec_collects_decorated_handlers(manager):
    builder = manager.controller(Pod)

    # Note: the decorators register as a side effect and return the
    # function unchanged; we apply them explicitly to keep references
    # for assertions.
    async def reconcile(client, request):
        pass

    def predicate(event):
        return True

    def startup(client):
        pass

    def shutdown(client):
        pass

    async def task(client):
        pass

    builder.reconcile(concurrency=3)(reconcile)
    builder.predicate()(predicate)
    builder.startup()(startup)
    builder.shutdown()(shutdown)
    builder.task()(task)
    builder.watch_owner(Pod)

    spec = builder.to_spec()

    assert isinstance(spec, ControllerSpec)
    assert spec.resource is Pod
    assert spec.reconcile is reconcile
    assert spec.concurrent_reconciles == 3
    assert spec.predicates == (predicate,)
    assert spec.startup is startup
    assert spec.shutdown is shutdown
    assert spec.tasks == (task,)
    assert len(spec.watches) == 1
    watch = spec.watches[0]
    assert watch.resource is Pod
    assert watch.kwargs == {'owner': Pod}


def test_registration_decorators_return_the_decorated_function(manager):
    builder = manager.controller(Pod)

    @builder.reconcile(concurrency=2)
    async def reconcile(client, request):
        pass

    @builder.predicate()
    def predicate(event):
        return True

    @builder.predicate  # no-parens form
    def another_predicate(event):
        return True

    @builder.startup()
    def startup(client):
        pass

    @builder.shutdown()
    def shutdown(client):
        pass

    @builder.task()
    async def task(client):
        pass

    @builder.watch(Pod)
    def on_pod_event(event):
        yield from ()

    assert all(
        callable(f)
        for f in (
            reconcile,
            predicate,
            another_predicate,
            startup,
            shutdown,
            task,
            on_pod_event,
        )
    )

    spec = builder.to_spec()
    assert spec.reconcile is reconcile
    assert spec.concurrent_reconciles == 2
    assert spec.predicates == (predicate, another_predicate)
    assert spec.startup is startup
    assert spec.shutdown is shutdown
    assert spec.tasks == (task,)
    assert spec.watches[0].handler is on_pod_event


def test_informer_decorators_return_the_decorated_function(manager):
    builder = manager.informer(Pod)

    @builder.stream()
    def receiver(event):
        pass

    @builder.transform
    def transformer(obj):
        return obj

    assert callable(receiver)
    assert callable(transformer)

    spec = builder.to_spec()
    assert spec.stream_receivers == (receiver,)
    assert spec.transformer is transformer


def test_index_decorator_returns_the_decorated_function(manager):
    builder = manager.controller(Pod)

    @builder.index('by-name')
    def by_name(obj):
        return [obj.metadata.name]

    assert callable(by_name)

    store = manager.store(Pod)
    pod = Pod(metadata=ObjectMeta(name='foo', namespace='default'))
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    store.add(pod)
    assert store.get_index('by-name')['foo'] == [pod]


def test_controller_spec_is_frozen(manager):
    builder = manager.controller(Pod)
    spec = builder.to_spec()
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.concurrent_reconciles = 99


def test_duplicate_reconcile_raises(manager):
    builder = manager.controller(Pod)
    builder.reconcile()(lambda client, request: None)

    with pytest.raises(Exception, match='reconcile'):
        builder.reconcile()(lambda client, request: None)


def test_duplicate_startup_raises(manager):
    builder = manager.controller(Pod)
    builder.startup()(lambda client: None)

    with pytest.raises(Exception, match='startup'):
        builder.startup()(lambda client: None)


def test_watch_decorator_registers_handler(manager):
    builder = manager.controller(Pod)

    def on_pod_event(event):
        yield from ()

    builder.watch(Pod)(on_pod_event)

    spec = builder.to_spec()
    assert len(spec.watches) == 1
    assert spec.watches[0].handler is on_pod_event


def test_informer_spec_collects_decorated_handlers(manager):
    builder = manager.informer(Pod)

    def transform(obj):
        return obj

    async def receiver(stream):
        pass

    builder.transform()(transform)
    builder.stream()(receiver)

    spec = builder.to_spec()

    assert isinstance(spec, InformerSpec)
    assert spec.resource is Pod
    assert spec.transformer is transform
    assert spec.stream_receivers == (receiver,)


def test_store_returns_live_indexer_with_index_decorator(manager):
    store = manager.store(Pod)
    assert isinstance(store, Indexer)

    def by_ip(obj):
        return []

    store.index('by_ip')(by_ip)

    assert 'by_ip' in store._indexers
