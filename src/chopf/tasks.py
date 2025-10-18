import anyio
from anyio import TASK_STATUS_IGNORED
from anyio.abc import TaskStatus
from anyio import get_cancelled_exc_class


class Task:
    """A task that can be awaited independent of a TaskGroup."""

    def __init__(self):
        self._running = anyio.Event()
        self._stop = anyio.Event()

    def reset_task(self):
        self._running = anyio.Event()
        self._stop = anyio.Event()

    @property
    def is_running(self):
        return self._running.is_set()

    @property
    def running(self):
        return self._running.wait()

    def __await__(self):
        return self._running.wait().__await__()

    async def __call__(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class ExampleTask(Task):
    def __init__(self):
        super().__init__()

    async def __call__(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        print(f'{self} is starting')
        try:
            print(f'{self} is running')

            # Inform any awaiters that we are ready.
            task_status.started()
            self._running.set()

            # Wait until told otherwise.
            await self._stop.wait()

        except get_cancelled_exc_class():
            # (perform cleanup)
            pass
            # Async cleanup work has to be done in a shielded cancel scope.
            # e.g.
            # with CancelScope(shield=True):
            #     await some_cleanup_function()

            # Always reraise the cancellation exception!
            raise
        finally:
            print(f'{self} is stopping')

    def stop(self):
        # Cancel the task group.
        if self._task_group:
            self._task_group.cancel_scope.cancel()
        # Or alterntatively set the stop event.
        #self._stop.set()

        # As anyio events can not be re-used we have to re-create them.
        self.reset_task()

