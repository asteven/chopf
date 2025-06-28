import copy
import datetime

from lightkube.resources.core_v1 import Pod

import chopf

from .resource import ChaosPod


# A controller that reconciles ChaosPod's.
chaos_ctl = chopf.controller(ChaosPod)

# Also watch for Pod's that are owned by this controller.
chaos_ctl.watch_owner(Pod)

# Register a async predicate.
@chaos_ctl.predicate()
async def ignore_updates(event):
    match type(event):
        case event.UpdateEvent:
            return False
    return True


# Asynchronous reconcile function.
@chaos_ctl.reconcile()
async def reconcile(
    client: chopf.Client, request: chopf.Request
):
    print(f'reconcile: {request}')

    # Get the chaospod we're currently reconciling.
    chaos_pod = await client.get(request)

    if chaos_pod.metadata.deletionTimestamp is not None:
        # If our chaos pod is being deleted there's nothing more we can do.
        return

    try:
        # If our child pod exists, check if it's time to delete it.
        pod = await client.get(Pod, name=request.name, namespace=request.namespace)
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        if chaos_pod.status.nextStop:
            if chaos_pod.status.nextStop < now:
                print('  deleting pod')
                print('')
                try:
                    await client.delete(
                        Pod, name=pod.metadata.name, namespace=pod.metadata.namespace
                    )
                except chopf.ApiError as e:
                    print(e)
                return
            else:
                # If it's not time for deletion yet, re-queue this request
                # to run at next stop time.
                print(f'    now: {now}')
                print(f'    nextStop: {chaos_pod.status.nextStop}')
                delta = chaos_pod.status.nextStop - now + datetime.timedelta(seconds=1)
                print(f'    delta: {delta}')
                print('')
                raise chopf.Requeue(after=delta.total_seconds())
    except chopf.ObjectNotFound:
        # Our child pod does not exist, we will create it below.
        pass

    # Create our pod
    print('  creating pod')
    pod = Pod(
        metadata=copy.deepcopy(chaos_pod.spec.template.metadata),
        spec=copy.deepcopy(chaos_pod.spec.template.spec),
    )
    pod.metadata.name = request.name
    pod.metadata.namespace = request.namespace
    # Set controller and owner reference.
    chaos_ctl.set_controller_reference(chaos_pod, pod)
    pod = await client.create(pod)

    # Update chaospod status
    print('  updating chaospod')
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    lifetime = chaos_pod.spec.lifetime
    chaos_pod.status.nextStop = now + datetime.timedelta(seconds=lifetime)
    chaos_pod.status.lastRun = pod.metadata.creationTimestamp
    chaos_pod = await client.update(chaos_pod)
    print('  after updating chaospod')
    print('')


if __name__ == '__main__':
    chopf.run(namespaces=['default'])
