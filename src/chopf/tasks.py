import anyio
from anyio import get_cancelled_exc_class


class Task:
    """A task that can be awaited independent of a TaskGroup."""

    def __init__(self):
        self._running = anyio.Event()

    @property
    def is_running(self):
        return self._running.is_set()

    @property
    def running(self):
        return self._running.wait()

    def __await__(self):
        return self._running.wait().__await__()

    async def __call__(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class ExampleTask(Task):
    def __init__(self):
        super().__init__()
        self._stop = anyio.Event()

    async def __call__(self):
        print(f'{self} is starting')
        try:
            print(f'{self} is running')
            self._running.set()
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
        self._stop.set()
