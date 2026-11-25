import datetime
import functools
import logging
import time

import anyio

from lightkube.core import resource as lkr
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Event
from lightkube.models.core_v1 import EventSource
from lightkube.models.core_v1 import ObjectReference
from lightkube.models.meta_v1 import ObjectMeta

from .exceptions import ApiObjectNotFound
from .controller import Request
from .observer import Observer
from .resources import identity


__all__ = [
    'Client',
]

log = logging.getLogger(__name__)


class Client:
    """Interface: End user interface to client and cache."""

    def _get_resource(self, request, resource):
        if request is not None:
            resource = request
        return resource

    def _get_resource_name_namespace(self, request=None, *, resource=None, name=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            name = request.name
            namespace = request.namespace
        elif isinstance(request, lkr.Resource):
            resource = type(request)
            name = request.metadata.name
            namespace = request.metadata.namespace
        else:
            resource = self._get_resource(request, resource)
        return resource, name, namespace

    def _is_watched_resource(self, resource, namespace=None):
        return all(
            (
                (resource in self.cache.resources),
                (namespace is None or namespace in self.cache.namespaces),
            )
        )

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

#    def _create_event(self, obj, type='Normal', reason='Updated', message=None):
#        now = datetime.datetime.now(datetime.timezone.utc)
#        obj_ref = ObjectReference(
#            apiVersion=obj.apiVersion,
#            kind=obj.kind,
#            name=obj.metadata.name,
#            namespace=obj.metadata.namespace,
#            resourceVersion=obj.metadata.resourceVersion,
#            uid=obj.metadata.uid,
#        )
#        _event = Event(
#            metadata=ObjectMeta(
#               namespace=obj.metadata.namespace,
#               generateName='todo-event-name-prefix',
#            ),
#            action='Action?',
#            type=type,
#            reason=reason,
#            message=message,
#            reportingComponent='todo-reporting_component',
#            reportingInstance='todo-reporting_instance',
#            # used in the "From" column in `kubectl describe`.
#            source=EventSource(component='todo-reporting_component', host='todo-controller-host'),
#            involvedObject=obj_ref,
#            firstTimestamp=now,  # seen in `kubectl describe ...`
#            lastTimestamp=now,  # seen in `kubectl get events`
#            eventTime=now,
#        )
#        return _event


class AsyncClient(Client):
    """Async end user interface to client and cache."""

    def __init__(self, api_client, cache, observer=None):
        self.api_client = api_client
        self.cache = cache
        self.observer = observer or Observer()

    async def _observed(self, verb, resource, call):
        """Run an API server call, reporting it to the observer."""
        start = time.monotonic()
        error = None
        try:
            return await call()
        except Exception as e:
            error = e
            raise
        finally:
            duration = time.monotonic() - start
            self.observer.on_api_call(verb, identity.gvk(resource), duration, error)

    async def get(self, request=None, *, resource=None, name=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
            name = request.name
        else:
            resource = self._get_resource(request, resource)
        if self._is_watched_resource(resource, namespace=namespace):
            return await self.cache.get(
                resource,
                namespace=namespace,
                name=name,
            )
        else:
            try:
                return await self._observed(
                    'get',
                    resource,
                    lambda: self.api_client.get(
                        resource, namespace=namespace, name=name
                    ),
                )
            except ApiError as e:
                raise ApiObjectNotFound(resource, name, namespace=namespace) from e

    async def list(self, request=None, *, resource=None, namespace=None):
        if isinstance(request, Request):
            resource = request.resource
            namespace = request.namespace
        else:
            resource = self._get_resource(request, resource)
        if self._is_watched_resource(resource, namespace=namespace):
            return await self.cache.list(
                resource,
                namespace=namespace,
            )
        else:
            async def _list():
                return [
                    obj
                    async for obj in self.api_client.list(
                        resource, namespace=namespace
                    )
                ]
            return await self._observed('list', resource, _list)

    async def create(self, resource):
        result = await self._observed(
            'create', resource, lambda: self.api_client.create(resource)
        )
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
        result = await self._observed(
            'update', resource, lambda: self.api_client.replace(resource)
        )
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
        resource, name, namespace = self._get_resource_name_namespace(
            request, resource=resource, name=name, namespace=namespace
        )
        if obj is None and isinstance(request, lkr.Resource):
            obj = request
        return await self._observed(
            'patch',
            resource,
            lambda: self.api_client.patch(
                resource, name=name, namespace=namespace, obj=obj
            ),
        )

    async def delete(self, request=None, *, resource=None, name=None, namespace=None):
        resource, name, namespace = self._get_resource_name_namespace(
            request, resource=resource, name=name, namespace=namespace
        )
        return await self._observed(
            'delete',
            resource,
            lambda: self.api_client.delete(
                resource, name=name, namespace=namespace
            ),
        )

#    async def event(self, resource, type='Normal', reason='Updated', message=None):
#        resource = get_resource(resource)
#        event = self._create_event(resource, type=type, reason=reason, message=message)
#        return await self._observed(
#            'create', Event, lambda: self.api_client.create(event)
#        )


def _sync_method(name):
    """Build a blocking wrapper around an AsyncClient method."""

    def method(self, *args, **kwargs):
        return anyio.from_thread.run(
            functools.partial(getattr(self._async_client, name), *args, **kwargs)
        )

    method.__name__ = name
    method.__doc__ = f'Blocking version of AsyncClient.{name}().'
    return method


class SyncClient:
    """Blocking adapter over AsyncClient.

    There is exactly one implementation of client behavior: AsyncClient.
    This adapter only changes the calling convention for sync
    reconcile/startup/shutdown functions running in a worker thread.
    """

    def __init__(self, async_client: AsyncClient):
        self._async_client = async_client

    def __repr__(self):
        return f'<SyncClient wrapping {self._async_client!r}>'


#for _name in ('get', 'list', 'create', 'update', 'patch', 'delete', 'event'):
for _name in ('get', 'list', 'create', 'update', 'patch', 'delete'):
    setattr(SyncClient, _name, _sync_method(_name))

