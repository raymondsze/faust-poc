import pytest
from unittest.mock import Mock, patch
from .agent import app, segment, SegmentMessage

@pytest.fixture()
def test_app(event_loop):
    app.finalize()
    app.conf.store = 'memory://'
    app.flow_control.resume()
    return app

@pytest.mark.asyncio()
async def test_accuracy():
    with patch('examples.agent.forward_topic') as mocked_forward_topic:
        mocked_forward_topic.send = mock_coro('OMG')
        async with segment.test_context() as agent:
            msg = SegmentMessage(input='input', output='output')
            event = await agent.put(msg)
            mocked_forward_topic.send.assert_called_with('hey')

def mock_coro(return_value=None, **kwargs):
    """Create mock coroutine function."""
    async def wrapped(*args, **kwargs):
        return return_value
    return Mock(wraps=wrapped, **kwargs)

async def run_tests():
    app.conf.store = 'memory://'   # tables must be in-memory
    await test_accuracy()

if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_tests())