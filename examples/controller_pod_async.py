import anyio

import chopf

from lightkube.resources.core_v1 import Pod


# This controller reonciles Pods.
podctl = chopf.controller(Pod)


@podctl.startup()
async def startup(client: chopf.Client):
    print('optional startup handler')


@podctl.shutdown()
async def shutdown(client: chopf.Client):
    print('optional shutdown handler')


# Register a async predicate.
# A predicate decides if the given event is added to the controllers
# workqueue (-> is reconciled) or not.
@podctl.predicate()
async def _predicate(event):
    print(f'async predicate: {event}')
    return True


# Register a sync predicate.
# Predicates can also be sync.
@podctl.predicate()
def _sync_predicate(event):
    print(f'sync predicate: {event}')
    return True


# Register a custom indexer on the controllers store.
# This indexes pods by their IPs.
@podctl.index('by_ip')
def index_by_ip(obj):
    ips = []
    try:
        pod_ips = obj.status.podIPs
        if pod_ips:
            ips = [item.ip for item in pod_ips]
    except AttributeError as e:
        print(e)
        pass
    finally:
        return ips


# Another indexer that indexes by laberls.
@podctl.index('by_label')
def index_by_label(obj):
    labels = []
    try:
        labels = obj.metadata.labels
    except AttributeError as e:
        print(e)
        pass
    finally:
        return labels


# Asynchronous reconcile function.
@podctl.reconcile(concurrency=1)
async def reconcile(client: chopf.Client, request: chopf.Request):
    print(f'reconcile: {request}')
    try:
        # Get the object we are reconciling from the cache.
        obj = await client.get(request)
        print(f'   reconciled pod: {obj}')
    except chopf.ObjectNotFound as e:
        # Note that if we don't catch and handle this exception
        # it is handled by the controller by discarding the event,
        # as there is no point in requeing the object it's not in the cache.
        print('   object no longer exists in cache')

    # Get a list of all objects which are of the same api_version and
    # kind as the request we are currently reconciling from the cache.
    obj_list = await client.list(request)
    print(f'   pod list: {obj_list}')

    # Use the index we defined above to get pods by their IP address.
    pods_by_ip = podctl.get_index('by_ip')
    print(f'   pods_by_ip: {pods_by_ip}: {list(pods_by_ip)}')
    print(f'   pods_by_ip keys: {pods_by_ip.keys()}')
    print(f'   pods_by_ip values: {pods_by_ip.values()}')
    print(f'   pods_by_ip items: {pods_by_ip.items()}')

    # Get pods indexed by label.
    pods_by_label = podctl.get_index('by_label')
    print(f'   pods_by_label: {pods_by_label}: {list(pods_by_label)}')

    print()
    await anyio.sleep(2)


if __name__ == '__main__':
    chopf.run()

