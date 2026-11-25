"""Tests for the Observer instrumentation seam.

Seam under test: a user-assigned Observer sees API calls through the
client facade, watch events through the informer, and reconcile outcomes
through the controller — with the no-op default keeping everything quiet.
"""

import anyio
import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.cache.index import Indexer
from chopf.cache.informer import Informer
from chopf.client import AsyncClient
from chopf.controller import Controller, Request
from chopf.exceptions import TemporaryError
from chopf.observer import Observer


class RecordingObserver(Observer):
    def __init__(self):
        self.api_calls = []
        self.watch_events = []
        self.reconciles = []

    def on_api_call(self, verb, gvk, duration, error=None):
        self.api_calls.append((verb, gvk, error))

    def on_watch_event(self, gvk, event_type):
        self.watch_events.append((gvk, event_type))

    def on_reconcile(self, gvk, request, duration, outcome):
        self.reconciles.append((gvk, request, outcome))


class DummyCache:
    def __init__(self):
        self.resources = set()
        self.namespaces = set()


class DummyAPI:
    async def get(self, resource, namespace=None, name=None):
        return object()


@pytest.fixture(autouse=True)
def ensure_pod_defaults():
    Pod.apiVersion = 'v1'
    Pod.kind = 'Pod'


@pytest.mark.anyio
async def test_api_calls_are_observed():
    observer = RecordingObserver()
    cache = DummyCache()
    client = AsyncClient(DummyAPI(), cache, observer=observer)

    await client.get(resource=Pod, name='foo', namespace='default')

    assert observer.api_calls == [('get', 'v1/Pod', None)]


@pytest.mark.anyio
async def test_watch_events_are_observed():
    observer = RecordingObserver()
    informer = Informer(
        api_client=object(),
        store=Indexer(),
        resource=Pod,
        namespace='default',
        observer=observer,
    )
    pod = Pod(metadata=ObjectMeta(name='foo', namespace='default', resourceVersion='1'))
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'

    await informer._add_or_update(pod)

    assert observer.watch_events == []  # dispatched events are not watch events
    await informer._process_event('ADDED', pod)
    assert observer.watch_events == [('v1/Pod', 'ADDED')]


@pytest.mark.anyio
async def test_reconcile_outcomes_are_observed():
    observer = RecordingObserver()

    class DummyCacheForController:
        synced = None

        def get_informers_by(self, resource):
            return []

    controller = Controller(
        async_client=object(),
        sync_client=object(),
        cache=DummyCacheForController(),
        resource=Pod,
        wait_for_cache=False,
        observer=observer,
    )

    tx, rx = anyio.create_memory_object_stream(10)

    class Queue:
        async def get(self, worker_num=None):
            return await rx.receive()

        async def forget(self, item):
            pass

        async def add_after(self, item, delay):
            pass

        async def done(self, item):
            pass

    controller.queue = Queue()

    async def failing_reconcile(client, request):
        raise TemporaryError('boom', delay=0)

    controller.reconcile = failing_reconcile
    request = Request(Pod, 'foo', namespace='ns')

    async with anyio.create_task_group() as tg:
        tg.start_soon(controller._reconciler, 0)
        await tx.send(request)
        with anyio.fail_after(1):
            while not observer.reconciles:
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    gvk, seen_request, outcome = observer.reconciles[0]
    assert gvk == 'v1/Pod'
    assert seen_request is request
    assert outcome == 'temporary'


def test_default_observer_is_quiet():
    observer = Observer()
    observer.on_api_call('get', 'v1/Pod', 0.1)
    observer.on_watch_event('v1/Pod', 'ADDED')
    observer.on_reconcile('v1/Pod', None, 0.1, 'success')
