import functools
import time
from config.logger import logger


class Base:
    def __init__(self) -> None:
        pass

    @staticmethod
    def timeit(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            t0 = time.time()
            result = func(*args, **kwargs)
            t1 = time.time()
            logger.warning(
                f"Function {func.__name__} executed in {t1 - t0:.4f} seconds"
            )
            return result

        return wrapper
