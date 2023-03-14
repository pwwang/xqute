from xqute import Xqute, logger

logger.setLevel('DEBUG')


async def main():
    xq = Xqute()
    await xq.put('sleep 5')
    await xq.put('sleep 5')
    await xq.run_until_complete()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
