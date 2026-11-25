import anyio
import anyio.lowlevel
import pytest

from chopf.tasks import ExampleTask, Task


@pytest.mark.anyio
async def test_task_requires_call_override() -> None:
    class DummyTask(Task):
        pass

    dummy = DummyTask()

    with pytest.raises(NotImplementedError):
        await dummy()

    with pytest.raises(NotImplementedError):
        dummy.stop()


@pytest.mark.anyio
async def test_example_task_lifecycle() -> None:
    task = ExampleTask()

    async with anyio.create_task_group() as tg:

        async def start_example():
            await task()

        tg.start_soon(start_example)
        await anyio.lowlevel.checkpoint()
        assert task.is_running

        tg.cancel_scope.cancel()
        await anyio.lowlevel.checkpoint()
        assert not task.is_running
