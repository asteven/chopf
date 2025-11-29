import anyio

import chopf


from lightkube.resources.core_v1 import Pod
from lightkube.resources.apps_v1 import ReplicaSet

# This controller reonciles ReplicaSets.
repset = chopf.controller(ReplicaSet)

# It also watches pods and reconciles their owning ReplicaSet.
repset.watch_owner(Pod)


# This controllers reconcile function.
@repset.reconcile(concurrency=1)
async def reconcile(client: chopf.Client, request: chopf.Request):
    print(f'reconcile: {request}')
    try:
        obj = await client.get(request)
        print(f'   {obj}')
    except chopf.ObjectNotFound as e:
        print('   object no longer exists in cache')

    # Simulate waiting for something ...
    await anyio.sleep(2)


if __name__ == '__main__':
    chopf.run()

