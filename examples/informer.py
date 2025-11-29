import chopf

from lightkube.resources.core_v1 import Pod


# Get the informer for Pods.
informer = chopf.informer(Pod)


# Register a transform function that changes the object in some way.
# e.g. strip 'managed fields' to reduce local memory footprint.
@informer.transform()
def transform(obj):
    print(f'transform: {obj}')
    return obj


# Receive a stream of events from the informer.
@informer.stream()
async def _stream(stream):
    """Receive a event stream from the informer.
    """
    async with stream:
        async for event in stream:
            print(f'stream: {event}')


# Another receiver of the same events.
@informer.stream()
async def _stream2(stream):
    """Receive a event stream from the informer.
    """
    async with stream:
        async for event in stream:

            print(f'stream2: {event}')


if __name__ == '__main__':
    chopf.run()

