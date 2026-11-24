import copy

import pytest
from lightkube.core import resource as lkr
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.cache.cache import Cache, ALL_NAMESPACES, is_namespaced_resource


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
    assert is_namespaced_resource(Pod)
