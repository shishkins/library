import asyncio
import secrets
from base64 import b64encode


async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))


def generate_api_token():
    return b64encode(secrets.token_hex(20).encode("utf-8"))
