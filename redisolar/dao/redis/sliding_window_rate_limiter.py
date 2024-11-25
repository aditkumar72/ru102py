# Uncomment for Challenge #7
import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def _get_key(self, name:str) -> str:
        """Get the key for the rate-limiter."""
        return self.key_schema.sliding_window_rate_limiter_key(
            name, self.window_size_ms, self.max_hits
        )

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        key = self._get_key(name)
        pipe = self.redis.pipeline()
        current_timestamp = datetime.datetime.now().timestamp() * 1000
        pipe.zadd(key, {f"{current_timestamp}-{random.random()}": str(current_timestamp)})
        pipe.zremrangebyscore(key, '-inf', current_timestamp - self.window_size_ms)
        pipe.zcard(key)
        hits = pipe.execute()[-1]
        if hits > self.max_hits:
            raise RateLimitExceededException()
        # END Challenge #7
