import anyio
import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.cache.events import CreateEvent, DeleteEvent, UpdateEvent
from chopf.cache.index import Indexer
from chopf.cache.informer import Informer


@pytest.fixture
def pod_factory():
    def factory(name: str, namespace: str | None = 'default', version: str = '1'):
        pod = Pod(metadata=ObjectMeta(name=name, namespace=namespace))
        pod.apiVersion = 'v1'
        pod.kind = 'Pod'
        pod.metadata.resourceVersion = version
        return pod

    return factory


@pytest.mark.anyio
async def test_informer_adds_object_and_dispatches_create_event(pod_factory):
    store = Indexer()
    informer = Informer(
        api_client=object(), store=store, resource=Pod, namespace='default'
    )
    tx, rx = anyio.create_memory_object_stream(10)
    informer.add_stream(tx)

    pod = pod_factory('foo')
    await informer._add_or_update(pod)

    with anyio.fail_after(1):
        event = await rx.receive()
    assert isinstance(event, CreateEvent)
    assert store.get(pod) is pod


@pytest.mark.anyio
async def test_informer_updates_object_and_dispatches_update_event(pod_factory):
    store = Indexer()
    informer = Informer(
        api_client=object(), store=store, resource=Pod, namespace='default'
    )
    tx, rx = anyio.create_memory_object_stream(10)
    informer.add_stream(tx)

    pod_v1 = pod_factory('foo', version='1')
    await informer._add_or_update(pod_v1)

    pod_v2 = pod_factory('foo', version='2')
    await informer._add_or_update(pod_v2)

    # first receive create, second update
    with anyio.fail_after(1):
        await rx.receive()  # CreateEvent
        event = await rx.receive()
    assert isinstance(event, UpdateEvent)
    assert event.old.metadata.resourceVersion == '1'
    assert event.new.metadata.resourceVersion == '2'


@pytest.mark.anyio
async def test_informer_delete_dispatches_delete_event(pod_factory):
    store = Indexer()
    informer = Informer(
        api_client=object(), store=store, resource=Pod, namespace='default'
    )
    tx, rx = anyio.create_memory_object_stream(10)
    informer.add_stream(tx)

    pod = pod_factory('foo')
    await informer._add_or_update(pod)
    await rx.receive()  # consume create

    await informer._delete(pod)

    with anyio.fail_after(1):
        event = await rx.receive()
    assert isinstance(event, DeleteEvent)
    assert pod.metadata.name not in {obj.metadata.name for obj in store.list()}
