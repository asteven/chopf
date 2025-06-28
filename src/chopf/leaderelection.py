from lightkube.resources.coordination_v1 import Lease
from lightkube.models.coordination_v1 import LeaseSpec  # noqa: F401

import chopf
from chopf import ObjectMeta  # noqa: F401


lease_ctl = chopf.controller(Lease, privileged=True)


@lease_ctl.startup()
async def startup(client: chopf.Client):
    print('STARTUP')
    # TODO: create lease object if it does not yet exist


@lease_ctl.reconcile(concurrency=1)
async def reconcile(client: chopf.Client, request: chopf.Request):
    print(f'reconcile: {request}')
    # TODO: start or stop things based on who the leader is
