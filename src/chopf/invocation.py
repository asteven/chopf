import asyncio
import functools

import anyio


def is_async_fn(fn) -> bool:
    if fn is None:
        return False
    elif isinstance(fn, functools.partial):
        return is_async_fn(fn.func)
    elif hasattr(fn, '__wrapped__'):  # @functools.wraps()
        return is_async_fn(fn.__wrapped__)
    else:
        return asyncio.iscoroutinefunction(fn)


def nonblocking(func):
    """Decorator that marks a given function as non blocking."""
    func.__nonblocking__ = True
    return func


async def invoke(func, *args, **kwargs):
    if is_async_fn(func):
        return await func(*args, **kwargs)
    else:
        if hasattr(func, '__nonblocking__'):
            return func(*args, **kwargs)
        else:
            return await anyio.to_thread.run_sync(
                functools.partial(func, *args, **kwargs)
            )
