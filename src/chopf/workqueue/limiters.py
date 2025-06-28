import dataclasses
import math
import sys
import time

from typing import Dict, List


class RateLimiter:
    # Interface

    def delay(self, item):
        raise NotImplementedError()

    def forget(self, item):
        raise NotImplementedError()

    def count(self, item):
        raise NotImplementedError()


@dataclasses.dataclass(init=False)
class MaxOfRateLimiter:
    limiters: List[RateLimiter]

    def __init__(self, *args):
        self.limiters = args

    def delay(self, item):
        return max([limiter.delay(item) for limiter in self.limiters])

    def forget(self, item):
        for limiter in self.limiters:
            limiter.forget(item)

    def count(self, item):
        return max([limiter.count(item) for limiter in self.limiters])


@dataclasses.dataclass
class BucketRateLimiter(RateLimiter):
    # Maximum number of tokens in the bucket
    capacity: int = 100
    # Rate of token addition per second
    rate: int = 10
    max_delay: float = 1000  # 1000 Seconds

    def __post_init__(self):
        self._tokens = self.capacity
        self._last_added = time.time()
        self._missing_tokens = 0

    def _add_tokens(self):
        # Add tokens to the bucket based on the elapsed time and rate
        now = time.time()
        tokens_to_add = int((now - self._last_added) * self.rate)
        if tokens_to_add > 0:
            self._tokens = min(self.capacity, self._tokens + tokens_to_add)
            self._last_added = now

    def delay(self, item):
        # Refill our bucket
        self._add_tokens()
        delay = 0
        if self._tokens > 0:
            # As we've got tokens, reset our missing tokens counter.
            self._missing_tokens = 0
            # Use a token for the current item.
            self._tokens -= 1
        else:
            # Increase our missing tokens counter.
            self._missing_tokens += 1
            # Calculcate a delay of 0.1 seconds per missing token.
            # See https://danielmangum.com/posts/controller-runtime-client-go-rate-limiting/
            delay = self._missing_tokens * 0.1
        return delay

    def forget(self, item):
        pass

    def count(self, item):
        return 0


@dataclasses.dataclass
class ItemExponentialFailureRateLimiter(RateLimiter):
    items: Dict[object, int] = dataclasses.field(default_factory=dict, init=False)
    base_delay: float = 0.005  # 5 Milliseconds
    max_delay: float = 1000  # 1000 Seconds

    def delay(self, item):
        current = self.items.get(item, 0)
        self.items[item] = current + 1
        backoff = self.base_delay * math.pow(2, current)
        if any((backoff > sys.maxsize, backoff > self.max_delay)):
            backoff = self.max_delay
        return backoff

    def forget(self, item):
        if item in self.items:
            del self.items[item]

    def count(self, item):
        return self.items.get(item, 0)
