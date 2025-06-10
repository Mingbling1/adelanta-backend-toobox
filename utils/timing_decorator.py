import time
from functools import wraps
from config.logger import logger


def timing_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        logger.debug(
            f"{func.__name__} took {end_time - start_time:.2f} seconds"
        )
        return result

    return wrapper
