import functools
import time


def ttl_cache(seconds: int):
    def decorator(func):
        value = None
        last_access = 0

        @functools.wraps(func)
        def inner(*args, **kwargs):
            nonlocal value, last_access
            now = time.time()

            if (now - seconds) >= last_access or value is None:
                value = func(*args, **kwargs)
                last_access = time.time()

            return value

        return inner

    return decorator


def hex_to_int(value: str):
    return int(value, 16)
