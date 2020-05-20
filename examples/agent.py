# examples/agent.py
import faust
import time
import sys
from copy import deepcopy

app_id = 'fano_speech_segmentation'
loop = asyncio.get_event_loop()
bootstrap_servers = os.getenv('KAFKA_SERVERS', 'kafka:9092').split(',')
session_timeout_ms = int(os.getenv('KAFKA_CONSUMER_SESSION_TIMEOUT_MS', 30000))
max_poll_interval_ms = int(os.getenv('KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS', 21600000))
rebalance_timeout_ms = int(os.getenv('KAFKA_CONSUMER_REBALANCE_TIMEOUT_MS', 60000))
max_request_size = int(os.getenv('KAFKA_PRODUCER_MSG_MAX_SIZE', 1073741824))
group_id = os.getenv('KAFKA_CONSUMER_GROUP_ID', app_id)
topic = os.getenv('KAFKA_TOPIC', 'fano_speech_segmentation')
ack_topic = os.getenv('KAFKA_ACK_TOPIC', 'fano_speech_segmentation.ack')
retry_topic = os.getenv('KAFKA_RETRY_TOPIC', 'fano_speech_segmentation.retry')
dlq_topic = os.getenv('KAFKA_DLQ_TOPIC', 'fano_speech_segmentation.dlq')
forward_topic = os.getenv('KAFKA_FORWARD_TOPIC', 'fano_speech_segmentation')
retry_limit = os.getenv('KAFAK_RETRY_LIMIT', 3)

# The model describes the data sent to our agent,
# We will use a JSON serialized dictionary
# with two integer fields: a, and b.
class Add(faust.Record):
    a: int
    b: int

# Next, we create the Faust application object that
# configures our environment.
app = faust.App(
    id=app_id,
    broker=bootstrap_servers,
    processing_guarantee='at_least_once',
    topic_replication_factor=1,
    topic_partitions=8,
    topic_allow_declare=True,
    broker_request_timeout=90,
    broker_session_timeout=session_timeout_ms,
    broker_heartbeat_interval=rebalance_timeout_ms,
    broker_max_poll_interval=max_poll_interval_ms,
    consumer_max_fetch_size=max_request_size,
    consumer_auto_offset_reset='earliest',
    producer_max_request_size=max_request_size,
)

# The Kafka topic used by our agent is named 'adding',
# and we specify that the values in this topic are of the Add model.
# (you can also specify the key_type if your topic uses keys).
topic = app.topic('adding', value_type=Add)
retry_topic = app.topic('adding_retry', value_type=Add)

@app.agent(retry_topic)
async def adding_retry(stream):d
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
