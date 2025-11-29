import time

import chopf

from lightkube.resources.core_v1 import Pod


# This controller reonciles Pods.
podctl = chopf.controller(Pod)


@podctl.startup()
def startup(client: chopf.Client):
    print('optional startup handler')


@podctl.shutdown()
def shutdown(client: chopf.Client):
    print('optional shutdown handler')


# Register a async predicate.
# A predicate decides if the given event is added to the controllers
# workqueue (-> is reconciled) or not.
@podctl.predicate()
def _predicate(event):
    print(f'sync predicate: {event}')
    return True


# Synchronous reconcile function.
@podctl.reconcile()
def reconcile(client: chopf.Client, request: chopf.Request):
    print(f'reconcile: {request}')
    try:
        # Get the object we are reconciling from the cache.
        obj = client.get(request)
        print(f'   reconciled pod: {obj}')
    except chopf.ObjectNotFound as e:
        # Note that if we don't catch and handle this exception
        # it is handled by the controller by discarding the event,
        # as there is no point in requeing the object it's not in the cache.
        print('   object no longer exists in cache')
    time.sleep(2)


if __name__ == '__main__':
    chopf.run()

