import anyio

from ..tasks import Task

from .limiters import (
    RateLimiter,  # noqa: F401
    MaxOfRateLimiter,
    BucketRateLimiter,
    ItemExponentialFailureRateLimiter,
)


class Queue:
    def __init__(self):
        self._items = {}

    def touch(self, item):
        self._items[item] = self._items[item] + 1

    def push(self, item):
        self._items[item] = 0

    def pop(self):
        k = next(iter(self._items))
        del self._items[k]
        return k

    def __contains__(self, item):
        return item in self._items

    def __len__(self):
        return len(self._items)


class Workqueue(Task):
    def __init__(self):
        super().__init__()
        self._rate_limiter = MaxOfRateLimiter(
            ItemExponentialFailureRateLimiter(), BucketRateLimiter()
        )
        self._buffer = []
        self._queue = Queue()
        self._delayed = {}
        self._processing = {}
        self._dirty = {}
        self._condition = anyio.Condition()
        self._stop = anyio.Event()

    def _print_statistics(self):
        print('workqueue statistics')
        print(f'    queued:     {self._queue._items}')
        print(f'    delayed:    {self._delayed}')
        print(f'    processing: {self._processing}')
        print(f'    dirty:      {self._dirty}')
        print('')

    def __len__(self):
        return len(self._queue)

    async def length(self):
        async with self._condition:
            return len(self._queue)

    def __repr__(self):
        length = len(self)
        delayed = len(self._delayed)
        dirty = len(self._dirty)
        processing = len(self._processing)
        if self.is_running:
            return f'<Workqueue queued: {length}, delayed: {delayed}, dirty: {dirty}, processing: {processing}>'
        else:
            buffered = len(self._buffer)
            return f'<Workqueue queued: {length}, delayed: {delayed}, dirty: {dirty}, processing: {processing} buffered: {buffered}>'

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.get()
        yield item
        await self.done(item)

    async def _add(self, item):
        """Add marks item as needing processing."""
        async with self._condition:
            if item in self._dirty:
                # The same item is added again before it is processed.
                # call the touch function for queues who care about it
                # to e.g. reset its priority
                if item not in self._processing:
                    self._queue.touch(item)
            else:
                self._dirty[item] = None
                if item not in self._processing:
                    self._queue.push(item)
                    ##self._condition.notify_all()
                    self._condition.notify()

    async def add(self, item):
        """Add marks item as needing processing."""
        if self.is_running:
            await self._add(item)
        else:
            # If the queue has not yet been started we buffer items
            # and add them during startup.
            self._buffer.append(item)

    async def get(self, worker_num=None):
        """Get blocks until it can return an item to be processed."""
        async with self._condition:
            while len(self) == 0:
                await self._condition.wait()
            item = self._queue.pop()
            self._processing[item] = None
            del self._dirty[item]
            #self._print_statistics()
            return item

    async def done(self, item):
        """Done marks item as done processing, and if it has been marked as dirty
        again while it was being processed, it will be re-added to the queue for
        re-processing.
        """
        async with self._condition:
            del self._processing[item]
            if item in self._dirty:
                self._queue.push(item)
                self._condition.notify()

    async def _add_after(self, item, delay):
        self._delayed[item] = delay
        await anyio.sleep(delay)
        await self.add(item)
        try:
            del self._delayed[item]
        except KeyError:
            # Ignore errors, we only do this for statistics.
            pass

    async def add_after(self, item, delay):
        # TODO: consider different delays
        # if item not in self._delayed:
        self._task_group.start_soon(self._add_after, item, delay)

    async def add_rate_limited(self, item):
        delay = self._rate_limiter.delay(item)
        if delay == 0:
            await self.add(item)
        else:
            self._task_group.start_soon(self._add_after, item, delay)

    async def forget(self, item):
        self._rate_limiter.forget(item)

    async def num_requeues(self, item):
        return self._rate_limiter.count(item)

    def shutdown(self):
        # needed?
        pass

    def shutdown_with_drain(self):
        # needed?
        pass

    def stop(self):
        self._stop.set()

    async def __call__(self):
        async with anyio.create_task_group() as tg:
            self._task_group = tg

            # Add any buffered items.
            while self._buffer:
                item = self._buffer.pop()
                await self._add(item)

            self._running.set()
            await self._stop.wait()
