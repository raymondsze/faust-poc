import os
import faust
import time
import sys
from uuid import uuid4

from typing import Any, List, Tuple
from faust.types import AppT
from mode.utils.compat import want_bytes

KAFKA_CLIENT_ID = 'fano_speech_segmentation'
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092').split(',')
KAFKA_BROKER_SESSION_TIMEOUT = int(os.getenv('KAFKA_BROKER_SESSION_TIMEOUT', 30))
KAFKA_BROKER_MAX_POLL_INTERVAL = int(os.getenv('KAFKA_BROKER_MAX_POLL_INTERVAL', 21600))
KAFKA_BROKER_HEARTBEAT_INTERVAL = int(os.getenv('KAFKA_BROKER_HEARTBEAT_INTERVAL', 60))
KAFKA_BROKER_REQUEST_TIMEOUT = int(os.getenv('KAFKA_BROKER_REQUEST_TIMEOUT', 90))
KAFKA_PRODUCER_MAX_REQUEST_SIZE = int(os.getenv('KAFKA_PRODUCER_MAX_REQUEST_SIZE', 1073741824))

KAFKA_CONSUMER_TOPIC_REPLICATION_FACTOR = os.getenv('KAFKA_CONSUMER_TOPIC_REPLICATION_FACTOR', 1)
KAFKA_CONSUMER_TOPIC_PARTITIONS = os.getenv('KAFKA_CONSUMER_TOPIC_TOTAL_PARTITIONS', 8)
KAFKA_CONSUMER_TOPIC = os.getenv('KAFKA_CONSUMER_TOPIC', 'fano_speech_segmentation')
# KAFKA_CONSUMER_ACK_TOPIC = os.getenv('KAFKA_CONSUMER_ACK_TOPIC', 'fano_speech_segmentation.ack')
KAFKA_CONSUMER_RETRY_TOPIC = os.getenv('KAFKA_CONSUMER_RETRY_TOPIC', 'fano_speech_segmentation.retry')
KAFAK_CONSUMER_DLQ_TOPIC = os.getenv('KAFAK_CONSUMER_DLQ_TOPIC', 'fano_speech_segmentation.dlq')
KAFKA_CONSUMER_FORWARD_TOPIC = os.getenv('KAFKA_CONSUMER_FORWARD_TOPIC', 'fano_speech_segmentation_2')
KAFAK_CONSUMER_RETRY_LIMIT = os.getenv('KAFAK_CONSUMER_RETRY_LIMIT', 3)
KAFKA_CONSUMER_MAX_FETCH_SIZE = int(os.getenv('KAFKA_CONSUMER_MAX_FETCH_SIZE', 1073741824))

# The model describes the data sent to our agent,
# We will use a JSON serialized dictionary
# with two str fields: input, and output.
class SegmentMessage(faust.Record):
    input: str
    output: str

response_schema = faust.Schema(
    key_type=SegmentMessage,
    value_type=SegmentMessage,
    key_serializer='json',
    value_serializer='json',
)

# Next, we create the Faust application object that
# configures our environment.
app = faust.App(
    id=KAFKA_CLIENT_ID,
    broker=KAFKA_BROKERS,
    processing_guarantee='at_least_once',
    topic_replication_factor=KAFKA_CONSUMER_TOPIC_REPLICATION_FACTOR,
    topic_partitions=KAFKA_CONSUMER_TOPIC_PARTITIONS,
    broker_request_timeout=KAFKA_BROKER_REQUEST_TIMEOUT,
    broker_session_timeout=KAFKA_BROKER_SESSION_TIMEOUT,
    broker_heartbeat_interval=KAFKA_BROKER_HEARTBEAT_INTERVAL,
    broker_max_poll_interval=KAFKA_BROKER_MAX_POLL_INTERVAL,
    consumer_max_fetch_size=KAFKA_CONSUMER_MAX_FETCH_SIZE,
    producer_max_request_size=KAFKA_PRODUCER_MAX_REQUEST_SIZE,
)

# The Kafka topic used by our agent,
# and we specify that the values in this topic are of the SegmentMessage model.
topic = app.topic(KAFKA_CONSUMER_TOPIC, value_type=SegmentMessage)
retry_topic = app.topic(KAFKA_CONSUMER_RETRY_TOPIC, value_type=SegmentMessage)
# ack_topic = app.topic(KAFKA_CONSUMER_ACK_TOPIC, value_type=SegmentMessage)
dlq_topic = app.topic(KAFAK_CONSUMER_DLQ_TOPIC, value_type=SegmentMessage)
forward_topic = None if KAFKA_CONSUMER_FORWARD_TOPIC is None else app.topic(KAFKA_CONSUMER_FORWARD_TOPIC, value_type=SegmentMessage)

@app.on_produce_message.connect()
def on_produce_attach_trace_headers(
    sender: AppT,
    key: bytes = None,
    value: bytes = None,
    partition: int = None,
    timestamp: float = None,
    headers: List[Tuple[str, bytes]] = None,
    **kwargs: Any
) -> None:
    headers_dict = dict(headers)
    message_id = headers_dict.get('Faust-Ag-CorrelationId')
    correlation_id = headers_dict.get('CorrelationId') or message_id
    causation_id = headers_dict.get('MessageId') or message_id
    status = headers_dict.get('Status')
    retry_count_header = headers_dict.get('RetryCount')
    retry_count = -1 if retry_count_header is None else int(retry_count_header.decode('utf-8'))
    headers.extend([
        ('CorrelationId', correlation_id),
        ('CausationId', causation_id),
        ('MessageId', message_id),
        ('RetryCount', bytes(str(retry_count + 1), 'utf-8')),
        ('Status', status or bytes('PENDING', 'utf-8')),
    ])

async def process_message(value: SegmentMessage):
    return value

@app.agent(forward_topic)
async def segment_2(stream):
    async for value in stream:
        print('Reply!')
        yield value

@app.agent(topic)
async def segment(stream):
    async for event in stream.events():
        print('Event Received')
        retry_count_bytes = event.headers.get('RetryCount')
        retry_count = 0 if retry_count_bytes is None else int(retry_count_bytes.decode('utf-8'))
        if retry_count > KAFAK_CONSUMER_RETRY_LIMIT:
            new_headers = event.headers.copy()
            new_headers['Status'] = bytes(str('ERROR'), 'utf-8')
            new_headers['ErrorMsg'] = bytes(str('RETRY_LIMIT_EXCEEDED'), 'utf-8')
            await event.forward(dlq_topic, headers=new_headers)
        try:
            value = await process_message(event.value)
            new_headers = event.headers.copy()
            new_headers['Status'] = bytes(str('SUCCESS'), 'utf-8')
            # if forward_topic is not None:
            #     await event.forward(forward_topic, headers=new_headers)
            #     # await event.send(forward_topic, key=event.key, value=value, headers=new_headers)
            # else:
            yield value.output
            print('Event Replied')
        except Exception as ex:
            new_headers = event.headers.copy()
            new_headers['ErrorMsg'] = bytes(str(ex), 'utf-8')
            await event.forward(retry_topic, headers=new_headers)

from faust.cli import argument, option

@app.command(
    argument('input', type=str),
    argument('output', type=str),
)
async def send_value(self, input: str, output: str) -> None:
    reply = await segment.ask(SegmentMessage(input, output))

