import logging

import anyio
from lightkube.resources.core_v1 import Namespace

import chopf
from ...controller import Controller


class NamespaceController(Controller):
    def __init__(self, manager, *args, **kwargs):
        self.manager = manager
        super().__init__(*args, Namespace, **kwargs)

    async def reconcile(self,
        client: chopf.Client, request: chopf.Request
    ):
        namespace = await client.get(request)
        name = namespace.metadata.name

        if namespace.metadata.deletionTimestamp is not None:
            #print('namespace is being deleted')
            await self.manager.remove_namespace(name)
            return

        if namespace.status.phase == 'Active':
            if name in self.manager.namespaces:
                await self.manager.add_namespace(name)
            else:
                await self.manager.remove_namespace(name)

