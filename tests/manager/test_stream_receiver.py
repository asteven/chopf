import anyio
import pytest

from chopf.manager.manager import StreamReceiver


class DummyResource:
    class _api_info:
        class resource:
            api_version = 'v1'
            kind = 'Foo'


@pytest.mark.anyio
async def test_stream_receiver_dispatches_to_handler():
    received = []

    async def handler(rx):
        async with rx:
            async for item in rx:
                received.append(item)

    receiver = StreamReceiver(resource=DummyResource, handler=handler)

    async with anyio.create_task_group() as tg:
        await tg.start(receiver)
        stream = receiver.stream
        await stream.send('hello')
        await stream.aclose()
        await anyio.sleep(0)
        assert received == ['hello']
        receiver.stop()
        tg.cancel_scope.cancel()
