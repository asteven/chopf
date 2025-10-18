import anyio

from lightkube.resources.apiextensions_v1 import CustomResourceDefinition
from lightkube.core.resource_registry import resource_registry
from lightkube.generic_resource import create_resources_from_crd

import chopf
from ...controller import Controller
from ...resources import custom_resource_registry


class CrdObserver(Controller):
    def __init__(self, manager, *args, **kwargs):
        self.manager = manager
        super().__init__(*args, **kwargs)

    async def reconcile(self,
        client: chopf.Client, request: chopf.Request
    ):
        crd = await client.get(request)

        if crd.metadata.deletionTimestamp is not None:
            #print('resource is being deleted')
            await self.remove_crd(crd)
            return

        ready = False
        if crd.status.conditions:
            ready = True
            for condition in crd.status.conditions:
                if not condition.status:
                    # We need all conditions to be true.
                    ready = False
                    break
        if ready:
            await self.add_crd(crd)

    async def add_crd(self, crd: CustomResourceDefinition):
        # Prefer explicit custom resources over lightkube's generic resources.
        if crd in custom_resource_registry:
            for resource in custom_resource_registry.get_resources(crd):
                if resource in self.manager.resources:
                    resource_registry.register(resource)
                    #if self.manager.is_active:
                        # Give the API server some time to serve the new CRD
                        # endpoints before we start hammering it.
                        # FIXME: This is a hack. Would need a better way to
                        #        figure out that the endpoint is usable.
                    #    await anyio.sleep(3)
                    await self.manager.add_resource(resource)
                else:
                    await self.manager.remove_resource(resource)
        else:
            create_resources_from_crd(crd)
            group = crd.spec.group
            kind = crd.spec.names.kind
            for version in crd.spec.versions:
                group_version = f'{group}/{version.name}'
                resource = resource_registry.load(group_version, kind)
                if resource in self.manager.resources:
                    await self.manager.add_resource(resource)
                else:
                    await self.manager.remove_resource(resource)

    async def remove_crd(self, crd: CustomResourceDefinition):
        group = crd.spec.group
        kind = crd.spec.names.kind
        for version in crd.spec.versions:
            group_version = f'{group}/{version.name}'
            resource = resource_registry.get(group_version, kind)
            await self.manager.remove_resource(resource)

