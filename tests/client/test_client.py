from types import SimpleNamespace

import anyio
import pytest
from lightkube.resources.core_v1 import Pod

from chopf.client import AsyncClient, SyncClient
from chopf.controller import Request
from chopf.exceptions import ApiObjectNotFound


class DummyCache:
    def __init__(self):
        self.resources = set()
        self.namespaces = set()
        self.get_calls = []
        self.list_calls = []

    async def get(self, resource, namespace=None, name=None):
        self.get_calls.append((resource, namespace, name))
        return SimpleNamespace(name=name)

    async def list(self, resource, namespace=None):
        self.list_calls.append((resource, namespace))
        return [SimpleNamespace(namespace=namespace)]


class DummyAsyncAPI:
    def __init__(self):
        self.get_calls = []
        self.list_calls = []
        self.create_calls = []
        self.replace_calls = []
        self.patch_calls = []
        self.delete_calls = []

    async def get(self, resource, namespace=None, name=None):
        self.get_calls.append((resource, namespace, name))
        return SimpleNamespace(name=name)

    async def list(self, resource, namespace=None):
        self.list_calls.append((resource, namespace))
        for i in range(2):
            yield SimpleNamespace(idx=i)

    async def create(self, resource):
        self.create_calls.append(resource)
        return resource

    async def replace(self, resource):
        self.replace_calls.append(resource)
        return resource

    async def patch(self, resource, name=None, namespace=None, obj=None):
        self.patch_calls.append((resource, name, namespace, obj))
        return obj

    async def delete(self, resource, name=None, namespace=None):
        self.delete_calls.append((resource, name, namespace))
        return {'deleted': name}


@pytest.fixture
def pod_resource():
    pod = Pod(metadata={'name': 'pod', 'namespace': 'default'})
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    return pod


@pytest.mark.anyio
async def test_async_client_get_prefers_cache(pod_resource):
    cache = DummyCache()
    cache.resources = {Pod}
    cache.namespaces = {'default'}
    api = DummyAsyncAPI()

    client = AsyncClient(api, cache)
    result = await client.get(request=Request(Pod, 'pod', namespace='default'))

    assert cache.get_calls == [(Pod, 'default', 'pod')]
    assert result.name == 'pod'
    assert api.get_calls == []


@pytest.mark.anyio
async def test_async_client_get_falls_back_to_api(pod_resource):
    cache = DummyCache()
    cache.resources = set()
    api = DummyAsyncAPI()

    client = AsyncClient(api, cache)
    result = await client.get(resource=Pod, name='pod', namespace='default')

    assert api.get_calls == [(Pod, 'default', 'pod')]
    assert result.name == 'pod'


@pytest.mark.anyio
async def test_async_client_get_raises_custom_on_api_error():
    cache = DummyCache()

    class FailingAPI(DummyAsyncAPI):
        async def get(self, *args, **kwargs):
            raise ApiObjectNotFound(Pod, 'pod', namespace='default')

    client = AsyncClient(FailingAPI(), cache)
    with pytest.raises(ApiObjectNotFound):
        await client.get(resource=Pod, name='pod', namespace='default')


@pytest.mark.anyio
async def test_sync_client_delegates_get_to_async_client(pod_resource):
    cache = DummyCache()
    cache.resources = {Pod}
    cache.namespaces = {'default'}
    api = DummyAsyncAPI()
    async_client = AsyncClient(api, cache)
    sync_client = SyncClient(async_client)

    request = Request(Pod, 'pod', namespace='default')
    result = await anyio.to_thread.run_sync(sync_client.get, request)

    assert result.name == 'pod'
    assert cache.get_calls == [(Pod, 'default', 'pod')]
    assert api.get_calls == []


@pytest.mark.anyio
async def test_sync_client_delegates_kwargs(pod_resource):
    cache = DummyCache()
    api = DummyAsyncAPI()
    async_client = AsyncClient(api, cache)
    sync_client = SyncClient(async_client)

    def call():
        return sync_client.patch(
            resource=Pod, name='pod', namespace='default', obj={'a': 1}
        )

    result = await anyio.to_thread.run_sync(call)

    assert result == {'a': 1}
    assert api.patch_calls == [(Pod, 'pod', 'default', {'a': 1})]
