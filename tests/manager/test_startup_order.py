"""Startup ordering: the internal runtime runs — and is synced — before
the leader payload.

Seams under test:
- InternalRuntime.start: cache started -> informers created -> controllers
  started -> initial list (reconcile) -> controllers drained (wait_idle).
- Manager.__call__: the internal runtime is awaited before the payload
  (manager.start) is scheduled.
- Controller.wait_idle: the drain primitive itself.
"""
import importlib

import anyio
import pytest
from lightkube.resources.apiextensions_v1 import CustomResourceDefinition
from lightkube.resources.core_v1 import Namespace, Pod

manager_mod = importlib.import_module('chopf.manager.manager')
system_mod = importlib.import_module('chopf.manager.system')

from chopf.controller import Controller, Request  # noqa: E402


class DummyCache:
    def __init__(self, api_client, stores=None):
        self.api_client = api_client
        self._all_namespaces = False
        self.namespaces = set()
        self.resources = set()
        self._stores = stores if stores is not None else {}

    def get_store(self, resource):
        return self._stores.setdefault(resource, object())


class DummyClient:
    namespace = 'default'

    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture(autouse=True)
def patch_manager_dependencies(monkeypatch):
    monkeypatch.setattr(manager_mod, 'LightkubeAsyncClient', DummyClient)
    monkeypatch.setattr(manager_mod, 'Cache', DummyCache)
    monkeypatch.setattr(system_mod, 'Cache', DummyCache)
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *a, **kw: None)


@pytest.fixture
def manager():
    return manager_mod.Manager()


@pytest.mark.anyio
async def test_internal_runtime_start_sequence(manager, monkeypatch):
    """start(): cache up -> informers created -> controllers up ->
    initial list -> controllers drained."""
    events = []

    class FakeCache:
        def __init__(self):
            self.resources = set()

        async def __call__(self, task_status):
            events.append('cache_start')
            task_status.started()
            await anyio.sleep_forever()

        def reconcile_informers(self):
            events.append('reconcile_informers')

        async def reconcile(self):
            events.append('cache_reconcile')

    class FakeController:
        async def __call__(self, task_status):
            events.append(f'controller_start:{id(self)}')
            task_status.started()
            await anyio.sleep_forever()

        async def wait_idle(self):
            events.append(f'wait_idle:{id(self)}')

    runtime = system_mod.InternalRuntime(manager)
    runtime.cache = FakeCache()
    controllers = [FakeController(), FakeController()]
    runtime._controllers = dict(zip([CustomResourceDefinition, Namespace], controllers))

    async with anyio.create_task_group() as tg:
        await runtime.start(tg)
        tg.cancel_scope.cancel()

    cache_events = ['cache_start', 'reconcile_informers']
    controller_starts = [f'controller_start:{id(c)}' for c in controllers]
    drains = [f'wait_idle:{id(c)}' for c in controllers]
    assert events == (
        cache_events + controller_starts + ['cache_reconcile'] + drains
    )


@pytest.mark.anyio
async def test_manager_call_starts_internal_runtime_before_payload(manager, monkeypatch):
    """Manager.__call__: internal runtime is awaited before the payload
    (manager.start) is scheduled."""
    events = []

    class FakeInternalRuntime:
        def __init__(self, manager):
            events.append('runtime_init')

        def _setup_controllers(self):
            events.append('runtime_setup')

        async def start(self, task_group):
            events.append('runtime_start')

    async def fake_manager_start():
        events.append('payload_start')

    monkeypatch.setattr(manager_mod, 'InternalRuntime', FakeInternalRuntime)
    monkeypatch.setattr(manager, 'start', fake_manager_start)

    async with anyio.create_task_group() as tg:
        tg.start_soon(manager.__call__, False, ['default'], False)
        # Wait until the payload has been scheduled.
        for _ in range(100):
            if 'payload_start' in events:
                break
            await anyio.sleep(0.01)
        assert events.index('runtime_start') < events.index('payload_start')
        manager._exit.set()
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_wait_idle_returns_immediately_when_queue_empty():
    controller = Controller(None, None, None, Pod)
    async with anyio.create_task_group() as tg:
        await tg.start(controller.queue)
        with anyio.fail_after(1):
            await controller.wait_idle()
        controller.queue.stop()


@pytest.mark.anyio
async def test_wait_idle_waits_for_queued_and_processing():
    controller = Controller(None, None, None, Pod)
    request = Request(Pod, 'pod', namespace='ns')
    idle = False

    async def waiter():
        nonlocal idle
        await controller.wait_idle()
        idle = True

    async with anyio.create_task_group() as tg:
        await tg.start(controller.queue)
        await controller.queue.add(request)
        tg.start_soon(waiter)

        # Queued: not idle.
        await anyio.sleep(0.2)
        assert not idle

        # Processing: still not idle.
        item = await controller.queue.get()
        await anyio.sleep(0.2)
        assert not idle

        # Done: the waiter completes.
        await controller.queue.done(item)
        await anyio.sleep(0.2)
        assert idle

        controller.queue.stop()
