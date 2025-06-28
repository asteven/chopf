import functools
import logging

import anyio

from .controller import Request
from .resources import get_resource

__all__ = [
    'Client',
]

log = logging.getLogger(__name__)


class Client:
    """Interface: End user interface to client and cache."""

    def _get_resource(self, request, resource):
        if request is not None:
            resource = request
        return get_resource(resource)

    def get(self, request=None, *, resource=None, name=None, namespace=None):
        """Get from cache or api server."""
        raise NotImplementedError()

    def list(self, request=None, *, resource=None, namespace=None):
        """List from cache or api server."""
        raise NotImplementedError()

    def create(self, resource=None):
        """Create in API server."""
        raise NotImplementedError()

    def update(self, resource=None):
        """Update on API server."""
        raise NotImplementedError()

    def patch(
        self, request=None, *, resource=None, name=None, namespace=None, obj=None
    ):
        """Patch on API server."""
        raise NotImplementedError()

    def delete(self, request=None, *, resource=None, name=None, namespace=None):
        """Delete from API server."""
        raise NotImplementedError()


class AsyncClient(Client):
    """Async end user interface to client and cache."""

    def __init__(self, api_client, cache):
        self.api_client = api_client
        self.cache = cache

    async def get(self, request=None, *, resource=None, name=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        if self.cache.is_watched_resource(resource, namespace):
            return await self.cache.get(
                resource,
                namespace=namespace,
                name=name,
            )
        else:
            return await self.api_client.get(
                resource,
                namespace=namespace,
                name=name,
            )

    async def list(self, request=None, *, resource=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
        else:
            resource = self._get_resource(request, resource)
        if self.cache.is_watched_resource(resource, namespace):
            return await self.cache.list(
                resource,
                namespace=namespace,
            )
        else:
            return [
                obj
                async for obj in self.api_client.list(
                    resource,
                    namespace=namespace,
                )
            ]

    async def create(self, resource):
        resource = get_resource(resource)
        result = await self.api_client.create(resource)
        if hasattr(resource, 'Status'):
            # TODO: is this sane? or to much magic?
            # If the resource has a Status sub resource also create it.
            if resource.status and resource.status != result.status:
                status_resource = resource.Status.from_dict(result.to_dict())
                status_resource.status = resource.status
                status_result = await self.api_client.replace(status_resource)
                result.status = status_result.status
        return result

    async def update(self, resource):
        resource = get_resource(resource)
        result = await self.api_client.replace(resource)
        if hasattr(resource, 'Status'):
            # TODO: is this sane? or to much magic?
            # Update the status subresouce if status has changed.
            if resource.status and resource.status != result.status:
                status_resource = resource.Status.from_dict(result.to_dict())
                status_resource.status = resource.status
                status_result = await self.api_client.replace(status_resource)
                result.status = status_result.status
        return result

    async def patch(
        self, request=None, *, resource=None, name=None, namespace=None, obj=None
    ):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        return await self.api_client.patch(
            resource, name=name, namespace=namespace, obj=obj
        )

    async def delete(self, request=None, *, resource=None, name=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        return await self.api_client.delete(resource, name=name, namespace=namespace)


class SyncClient(Client):
    """Sync end user interface to client and cache."""

    def __init__(self, api_client, cache):
        self.api_client = api_client
        self.cache = cache

    def get(self, request=None, *, resource=None, name=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        if self.cache.is_watched_resource(resource, namespace):
            return anyio.from_thread.run(
                functools.partial(
                    self.cache.get,
                    resource,
                    namespace=namespace,
                    name=name,
                )
            )
        else:
            return self.api_client.get(
                resource,
                namespace=namespace,
                name=name,
            )

    def list(self, request=None, *, resource=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
        else:
            resource = self._get_resource(request, resource)
        if self.cache.is_watched_resource(resource, namespace):
            return anyio.from_thread.run(
                functools.partial(
                    self.cache.list,
                    resource,
                    namespace=namespace,
                )
            )
        else:
            return [
                obj
                for obj in self.api_client.list(
                    resource,
                    namespace=namespace,
                )
            ]

    def create(self, resource):
        resource = get_resource(resource)
        result = self.api_client.create(resource)
        if hasattr(resource, 'Status'):
            # If the resource has a Status sub resource also create it.
            if resource.status and resource.status != result.status:
                status_resource = resource.Status.from_dict(result.to_dict())
                status_resource.status = resource.status
                status_result = self.api_client.replace(status_resource)
                result.status = status_result.status
        return result

    def update(self, resource):
        resource = get_resource(resource)
        result = self.api_client.replace(resource)
        if hasattr(resource, 'Status'):
            # Update the status subresouce if status has changed.
            if resource.status and resource.status != result.status:
                status_resource = resource.Status.from_dict(result.to_dict())
                status_resource.status = resource.status
                status_result = self.api_client.replace(status_resource)
                result.status = status_result.status
        return result

    def patch(
        self, request=None, *, resource=None, name=None, namespace=None, obj=None
    ):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        return self.api_client.patch(resource, name=name, namespace=namespace, obj=obj)

    def delete(self, request=None, *, resource=None, name=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        return self.api_client.delete(resource, name=name, namespace=namespace)
