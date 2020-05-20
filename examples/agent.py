# examples/agent.py
import faust
import time
import sys
from copy import deepcopy

# The model describes the data sent to our agent,
# We will use a JSON serialized dictionary
# with two integer fields: a, and b.
class Add(faust.Record):
    a: int
    b: int

# Next, we create the Faust application object that
# configures our environment.
app = faust.App('agent-example', broker='kafka://localhost:9092')

# The Kafka topic used by our agent is named 'adding',
# and we specify that the values in this topic are of the Add model.
# (you can also specify the key_type if your topic uses keys).
topic = app.topic('adding', value_type=Add)
retry_topic = app.topic('adding_retry', value_type=Add)

@app.agent(retry_topic)
async def adding_retry(stream):
    async for event in stream.events():
        print('Retry Topic')
        value = event.value
        print(event.headers)
        # here we receive Add objects, add a + b.
        yield value.a + value.b

@app.agent(topic)
async def adding(stream):
    async for event in stream.events():
        retry_count_header = event.headers.get('Retry-Count')
        retry_count = 0 if retry_count_header is None else int(retry_count_header.decode('utf-8'))
        if retry_count > 3:
            await event.forward('adding_dlq', headers=new_headers)
        try:
            value = event.value
            yield value
        except:
            new_headers = event.headers.copy()
            new_headers['Retry-Count'] = bytes(str(1), 'utf-8')
            await event.forward('adding_retry', headers=new_headers)

from faust.cli import argument, option

# @app.command(
#     # argument('a', type=int, help='First number to add'),
#     # argument('b', type=int, help='Second number to add'),
#     option('--a', type=int, default=4, help='First number to add'),
#     option('--b', type=int, default=4, help='Second number to add'),
#     option('--print/--no-print'),
# )
# async def send_value(a: int, b: int, print: bool) -> None:
#     if print:
#         print(f'Sending Add({x}, {y})...')
#     print(await adding.ask(Add(a, b)))

from faust.cli import argument, option

# @app.timer(1)
# async def produce():
#     await adding.cast(value=Add(4, 4))

@app.command(
    argument('a', type=int),
    argument('b', type=int),
    # option('--print/--no-print', help='Enable debug output'),
)
async def send_value(self, a: int, b: int) -> None:
    # if print:
    #     print(f'Sending Add({x}, {y})...')
    print(await adding.ask(Add(a, b)))
