from collections import defaultdict

import anyio
import pytest
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources.core_v1 import Pod

from chopf.controller import Controller, Request
from chopf.exceptions import (
    Error,
    ObjectNotFound,
    PermanentError,
    Requeue,
    TemporaryError,
)


class DummyInformer:
    def __init__(self):
        self.streams = {}

    def has_stream(self, key):
        return key in self.streams

    def add_stream(self, stream, key):
        self.streams[key] = stream


class DummyCache:
    def __init__(self):
        self.synced = anyio.Event()
        self._informers = defaultdict(list)

    def add_informer(self, resource, informer):
        self._informers[resource].append(informer)

    def get_informers_by(self, resource):
        return list(self._informers.get(resource, []))

    def get_store(self, resource):
        raise NotImplementedError


class StubQueue:
    def __init__(self, num_requeues_value=0):
        self._tx, self._rx = anyio.create_memory_object_stream(10)
        self.num_requeues_value = num_requeues_value
        self.add_calls = []
        self.add_after_calls = []
        self.rate_limited_calls = []
        self.forget_calls = []
        self.done_calls = []
        self._done_event = anyio.Event()

    async def push(self, request):
        await self._tx.send(request)

    async def get(self, worker_num=None):
        return await self._rx.receive()

    async def add(self, item):
        self.add_calls.append(item)

    async def add_after(self, item, delay):
        self.add_after_calls.append((item, delay))

    async def add_rate_limited(self, item):
        self.rate_limited_calls.append(item)

    async def forget(self, item):
        self.forget_calls.append(item)

    async def num_requeues(self, item):
        return self.num_requeues_value

    async def done(self, item):
        self.done_calls.append(item)
        self._done_event.set()

    async def wait_done(self, timeout=1):
        with anyio.fail_after(timeout):
            await self._done_event.wait()
        self._done_event = anyio.Event()

    async def shutdown(self):
        self._rx.close()
        await self._tx.aclose()


@pytest.fixture(autouse=True)
def ensure_pod_defaults():
    Pod.apiVersion = 'v1'
    Pod.kind = 'Pod'


@pytest.fixture
async def base_controller():
    cache = DummyCache()
    informer = DummyInformer()
    cache.add_informer(Pod, informer)
    cache.synced.set()

    controller = Controller(
        async_client=object(),
        sync_client=object(),
        cache=cache,
        resource=Pod,
        reconcile=lambda client, request: None,
        wait_for_cache=False,
    )
    return controller


@pytest.mark.anyio
async def test_controller_adds_default_watch(base_controller):
    assert base_controller.event_sources
    assert base_controller.event_sources[0].resource is base_controller.resource


@pytest.mark.anyio
async def test_reconciler_success_flow(base_controller):
    queue = StubQueue()
    base_controller.queue = queue

    processed = []

    async def reconcile(client, request):
        processed.append(request)

    base_controller.reconcile = reconcile

    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert processed == [request]
        assert queue.forget_calls == [request]
        assert queue.done_calls == [request]
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_reconciler_object_not_found_forgets(base_controller):
    queue = StubQueue()
    base_controller.queue = queue

    async def reconcile(client, request):
        obj = Pod(metadata=ObjectMeta(name=request.name, namespace=request.namespace))
        raise ObjectNotFound(obj)

    base_controller.reconcile = reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert queue.forget_calls == [request]
        assert queue.add_after_calls == []
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_reconciler_permanent_error_forgets(base_controller):
    queue = StubQueue()
    base_controller.queue = queue

    async def reconcile(client, request):
        raise PermanentError('nope')

    base_controller.reconcile = reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert queue.forget_calls == [request]
        assert not queue.add_calls
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_reconciler_temporary_error_requeues_after_delay(base_controller):
    queue = StubQueue()
    base_controller.queue = queue

    async def reconcile(client, request):
        raise TemporaryError('later', delay=5)

    base_controller.reconcile = reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert queue.add_after_calls == [(request, 5)]
        assert queue.forget_calls == [request]
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_reconciler_requeue_without_delay(base_controller):
    queue = StubQueue()
    base_controller.queue = queue

    async def reconcile(client, request):
        raise Requeue()

    base_controller.reconcile = reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert queue.add_calls == [request]
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_reconciler_requeue_with_delay(base_controller):
    queue = StubQueue()
    base_controller.queue = queue

    async def reconcile(client, request):
        raise Requeue(after=3)

    base_controller.reconcile = reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert queue.add_after_calls == [(request, 3)]
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_reconciler_unexpected_error_rate_limits(base_controller):
    queue = StubQueue(num_requeues_value=7)
    base_controller.queue = queue

    async def reconcile(client, request):
        raise RuntimeError('boom')

    base_controller.reconcile = reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(base_controller._reconciler, 0)
        await queue.push(request)
        await queue.wait_done()
        assert queue.rate_limited_calls == [request]
        assert request.retries == 7
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_set_owner_reference_flags():
    owner = Pod(metadata=ObjectMeta(name='owner', namespace='default', uid='uid'))
    subject = Pod(metadata=ObjectMeta(name='child', namespace='default'))

    controller = Controller(
        async_client=object(),
        sync_client=object(),
        cache=DummyCache(),
        resource=Pod,
        reconcile=lambda c, r: None,
    )

    ref = controller.set_owner_reference(owner, subject, controller=True)
    assert isinstance(ref, OwnerReference)
    assert subject.metadata.ownerReferences[0].name == 'owner'


@pytest.mark.anyio
async def test_set_controller_reference_conflict():
    owner = Pod(metadata=ObjectMeta(name='owner', namespace='default', uid='uid'))
    subject = Pod(metadata=ObjectMeta(name='child', namespace='default'))
    subject.metadata.ownerReferences = [
        OwnerReference(
            apiVersion='v1', kind='Pod', name='other', uid='1', controller=True
        )
    ]

    controller = Controller(
        async_client=object(),
        sync_client=object(),
        cache=DummyCache(),
        resource=Pod,
        reconcile=lambda c, r: None,
    )

    with pytest.raises(Error):
        controller.set_controller_reference(owner, subject)
