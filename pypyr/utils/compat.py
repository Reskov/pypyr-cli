import asyncio
import sys

py37 = sys.version_info >= (3, 7)


def async_run(future):
    if py37:
        return asyncio.run(future)
    else:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(future)


async def ensure_future_result(result):
    if asyncio.iscoroutine(result):
        return await result
    return result
