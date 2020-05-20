import asyncio
from faust.cli import option
from .agent import Add, adding

async def send_value() -> None:
    value = await adding.cast(Add(a=4, b=4))
    print(value)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_value())