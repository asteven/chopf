import itertools
import logging

from ..resources import identity

from ..invocation import nonblocking

from lightkube.codecs import resource_registry


log = logging.getLogger(__name__)


class Request:
    resource: object
    name: str
    namespace: str = None
    retries: int = 0

    def __init__(self, resource, name, namespace=None):
        self.resource = resource
        self.name = name
        self.namespace = namespace
        self.retries = 0

    # NB: self.resource is usually a resource *class*. Under msgspec,
    # class-level field access returns member descriptors, so identity is
    # always resolved via the identity helpers (api_info seam).
    @property
    def api_version(self) -> str:
        return identity.api_version(self.resource)

    @property
    def kind(self) -> str:
        return identity.kind(self.resource)

    def __hash__(self):
        return hash(
            (self.api_version, self.kind, self.namespace, self.name)
        )

    def __eq__(self, other):
        return (
            self.api_version,
            self.kind,
            self.namespace,
            self.name,
        ) == (
            other.api_version,
            other.kind,
            other.namespace,
            other.name,
        )

    def __repr__(self):
        if self.namespace is not None:
            name = f'{self.namespace}/{self.name}'
        else:
            name = self.name
        return f'<Request {self.api_version}/{self.kind} {name} retries: {self.retries}>'


@nonblocking
def requests_from_event_for_object(event):
    match type(event):
        case event.CreateEvent | event.DeleteEvent:
            return request_for_object(event.obj)
        case event.UpdateEvent:
            generators = []
            if event.old is not None:
                generators.append(request_for_object(event.old))
            if event.new is not None:
                generators.append(request_for_object(event.new))
            return itertools.chain(*generators)


@nonblocking
def requests_from_event_for_owner(event, owner=None):
    match type(event):
        case event.CreateEvent | event.DeleteEvent:
            return request_for_owner(event.obj, owner=owner)
        case event.UpdateEvent:
            generators = []
            if event.old is not None:
                generators.append(request_for_owner(event.old, owner=owner))
            if event.new is not None:
                generators.append(request_for_owner(event.new, owner=owner))
            return itertools.chain(*generators)


def request_for_object(obj):
    if obj is None:
        return
    namespace = obj.metadata.namespace
    name = obj.metadata.name
    resource = resource_registry.load(obj.apiVersion, obj.kind)
    yield Request(resource, name, namespace=namespace)


def request_for_owner(obj, owner=None):
    if obj is None:
        return
    namespace = obj.metadata.namespace
    name = obj.metadata.name
    try:
        refs = obj.metadata.ownerReferences
        if refs:
            for ref in refs:
                if (
                    ref.apiVersion == identity.api_version(owner)
                    and ref.kind == identity.kind(owner)
                    and ref.controller
                ):
                    name = ref.name
                    yield Request(owner, name, namespace=namespace)
    except AttributeError as e:
        raise e
        pass
