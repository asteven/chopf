import anyio
import pytest

from chopf.workqueue import Workqueue


@pytest.mark.anyio
async def test_workqueue_processes_buffered_items() -> None:
    queue = Workqueue()
    await queue.add('foo')  # buffered while queue not yet running

    async with anyio.create_task_group() as tg:
        await tg.start(queue)
        assert await queue.length() == 1

        with anyio.fail_after(1):
            item = await queue.get()
        assert item == 'foo'

        await queue.done(item)

        queue.stop()
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_workqueue_requeues_dirty_items_after_done() -> None:
    queue = Workqueue()

    async with anyio.create_task_group() as tg:
        await tg.start(queue)
        await queue.add('foo')

        with anyio.fail_after(1):
            first = await queue.get()
        assert first == 'foo'

        # Mark item dirty again while processing
        await queue.add('foo')
        # Completing the first processing should requeue the item
        await queue.done(first)

        with anyio.fail_after(1):
            second = await queue.get()
        assert second == 'foo'

        await queue.done(second)

        queue.stop()
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_workqueue_add_after_schedules_items_later() -> None:
    queue = Workqueue()

    async with anyio.create_task_group() as tg:
        await tg.start(queue)
        await queue.add_after('delayed', 0.05)

        # Item should appear after delay
        with anyio.fail_after(1):
            item = await queue.get()
        assert item == 'delayed'

        await queue.done(item)

        queue.stop()
        tg.cancel_scope.cancel()
