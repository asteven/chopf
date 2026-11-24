import anyio
import pytest
from lightkube.resources.core_v1 import Pod

from chopf.source import EventSource


class RecordingQueue:
    """Minimal async queue stub collecting items passed via add."""

    def __init__(self):
        self.added: list[object] = []
        self._event = anyio.Event()

    async def add(self, item: object) -> None:
        self.added.append(item)
        self._event.set()

    async def wait_for_items(self, count: int) -> None:
        while len(self.added) < count:
            await self._event.wait()
            self._event = anyio.Event()


class DummyEvent:
    def __init__(self, payload: str):
        self.payload = payload

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f'DummyEvent({self.payload})'


@pytest.mark.anyio
async def test_event_source_dispatches_requests_when_predicates_pass() -> None:
    queue = RecordingQueue()
    handler_calls: list[str] = []

    async def handler(event: DummyEvent, suffix: str) -> list[str]:  # type: ignore[override]
        result = f'{event.payload}{suffix}'
        handler_calls.append(result)
        return [result]

    async def predicate(event: DummyEvent) -> bool:  # type: ignore[override]
        return True

    source = EventSource(
        queue=queue,
        resource=Pod,
        handler=handler,
        kwargs={'suffix': '-ok'},
        predicates=[predicate],
    )

    async with anyio.create_task_group() as tg:
        await tg.start(source)
        stream = source.stream
        await stream.send(DummyEvent('evt1'))
        with anyio.fail_after(1):
            await queue.wait_for_items(1)
        assert queue.added == ['evt1-ok']
        assert handler_calls == ['evt1-ok']
        stream.close()
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_event_source_skips_events_on_failing_predicate() -> None:
    queue = RecordingQueue()
    handler_invoked = anyio.Event()

    async def handler(event: DummyEvent) -> list[str]:  # type: ignore[override]
        handler_invoked.set()
        return [event.payload]

    async def predicate(event: DummyEvent) -> bool:  # type: ignore[override]
        return False

    source = EventSource(
        queue=queue,
        resource=Pod,
        handler=handler,
        predicates=[predicate],
    )

    async with anyio.create_task_group() as tg:
        await tg.start(source)
        stream = source.stream
        await stream.send(DummyEvent('ignored'))
        await anyio.sleep(0.05)
        assert queue.added == []
        assert handler_invoked.is_set() is False
        stream.close()
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_event_source_ignores_empty_handler_results() -> None:
    queue = RecordingQueue()

    async def handler(event: DummyEvent) -> None:  # type: ignore[override]
        return None

    source = EventSource(
        queue=queue,
        resource=Pod,
        handler=handler,
    )

    async with anyio.create_task_group() as tg:
        await tg.start(source)
        stream = source.stream
        await stream.send(DummyEvent('noop'))
        await anyio.sleep(0.05)
        assert queue.added == []
        stream.close()
        tg.cancel_scope.cancel()


def test_event_source_stream_before_start_raises() -> None:
    queue = RecordingQueue()

    async def handler(event: DummyEvent) -> list[str]:  # type: ignore[override]
        return [event.payload]

    source = EventSource(
        queue=queue,
        resource=Pod,
        handler=handler,
    )

    with pytest.raises(RuntimeError):
        _ = source.stream
