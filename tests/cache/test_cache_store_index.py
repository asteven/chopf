import copy

import pytest
from lightkube.core import resource as lkr
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.cache.cache import Cache, ALL_NAMESPACES
from chopf.resources import identity


def build_pod(name: str, namespace: str | None = None):
    pod = Pod(metadata=ObjectMeta(name=name, namespace=namespace))
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    return pod


class DummyApiClient:
    pass


@pytest.fixture(autouse=True)
def ensure_pod_defaults():
    Pod.apiVersion = 'v1'
    Pod.kind = 'Pod'


@pytest.mark.anyio
async def test_cache_get_returns_deepcopy():
    cache = Cache(api_client=DummyApiClient())
    cache.resources = {Pod}
    cache.namespaces = {'default'}

    pod = build_pod('foo', 'default')
    cache.get_store(Pod).add(pod)

    fetched = await cache.get(Pod, namespace='default', name='foo')
    assert fetched is not pod
    fetched.metadata.labels = {'mutated': 'yes'}
    original = cache.get_store(Pod).get(pod)
    assert getattr(original.metadata, 'labels', None) is None


@pytest.mark.anyio
async def test_cache_list_namespace_filter():
    cache = Cache(api_client=DummyApiClient())
    cache.resources = {Pod}
    cache.namespaces = {'default'}

    pod = build_pod('foo', 'default')
    cache.get_store(Pod).add(pod)

    listed = await cache.list(Pod, namespace='default')
    assert len(listed) == 1
    assert listed[0].metadata.name == 'foo'


@pytest.mark.anyio
async def test_cache_namespaces_purge_removed_namespace():
    cache = Cache(api_client=DummyApiClient())
    cache.resources = {Pod}
    cache.namespaces = {'ns1', 'ns2'}

    store = cache.get_store(Pod)
    store.add(build_pod('a', 'ns1'))
    store.add(build_pod('b', 'ns2'))

    cache.namespaces = {'ns2'}

    remaining = await cache.list(Pod)
    names = {pod.metadata.name for pod in remaining}
    assert names == {'b'}


@pytest.mark.anyio
async def test_cache_resources_setter_clears_removed_resource_store():
    cache = Cache(api_client=DummyApiClient())
    cache.resources = {Pod}
    store = cache.get_store(Pod)
    store.add(build_pod('a', 'ns1'))

    cache.resources = set()

    assert list(store.list()) == []


def test_is_namespaced_resource():
    assert identity.is_namespaced(Pod)


@pytest.mark.anyio
async def test_cache_list_returns_deepcopies():
    """Mutating objects returned by cache.list must not corrupt the store."""
    cache = Cache(DummyApiClient())
    store = cache.get_store(Pod)
    store.add(build_pod('p1', 'ns1'))

    listed = await cache.list(Pod, namespace='ns1')
    listed[0].metadata.name = 'MUTATED'

    fresh = await cache.list(Pod, namespace='ns1')
    assert fresh[0].metadata.name == 'p1'


@pytest.mark.anyio
async def test_cache_reconcile_without_task_group_is_not_synced():
    """If the cache is not running, informers cannot start, so reconcile()
    must not report the cache as synced (controllers would reconcile
    against an empty store)."""
    cache = Cache(DummyApiClient())
    assert cache._task_group is None

    await cache.reconcile()

    assert not cache._synced.is_set()
