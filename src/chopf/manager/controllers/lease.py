import logging

import anyio

from lightkube.resources.coordination_v1 import Lease
from lightkube.models.coordination_v1 import LeaseSpec  # noqa: F401


import chopf
from ...controller import Controller


class LeaseController(Controller):
    def __init__(self, manager, *args, **kwargs):
        self.manager = manager
        super().__init__(*args, **kwargs)

    async def startup(self, client: chopf.Client):
        print('lease controller start')
        # TODO: create lease object if it does not yet exist

    async def reconcile(self,
        client: chopf.Client, request: chopf.Request
    ):
        print(f'reconcile: {request}')
        # TODO: start or stop things based on who the leader is

        lease = await client.get(request)

        if lease.metadata.deletionTimestamp is not None:
            print('lease is being deleted')
            return
