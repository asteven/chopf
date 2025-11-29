import anyio
import random

from lightkube.resources.core_v1 import Pod

import chopf

# This controller reonciles Pods.
podctl = chopf.controller(Pod)


# Asynchronous reconcile function.
@podctl.reconcile
async def reconcile(client: chopf.Client, request: chopf.Request):
    print(f'reconcile: {request}')
    obj = await client.get(request)
    print(f'   {obj}')
    print(f'   retries: {request.retries} {request.namespace}/{request.name}')

    # Randomly simulate exceptions.
    exception = random.choice((
        None,
        chopf.TemporaryError('Retry in 5 seconds.', delay=5),
        chopf.Error('An unexpected error occured.'),
    ))

    if exception is not None:
        raise exception

    await anyio.sleep(2)


if __name__ == '__main__':
    chopf.run()

