from dataclasses import dataclass

from lightkube.core import resource as lkr
from lightkube.models import meta_v1


# Monkeypatch lightkube Resources __repr__ to something more useful.
def _resource__repr__(self):
    # print(type(self))
    api_version = self.apiVersion
    kind = self.kind
    # name = getattr(self.metadata, 'name', self.metadata.get('name', None))
    # namespace = getattr(self.metadata, 'namespace', self.metadata.get('namespace', None))
    name = self.metadata.name
    namespace = self.metadata.namespace
    resource_version = getattr(self.metadata, 'resourceVersion', None)
    out = []
    out.append(f'{api_version}/{kind}')
    if namespace is not None:
        out.append(f'{namespace}/{name}')
    elif name is not None:
        out.append(f'{name}')
    if resource_version is not None:
        out.append(resource_version)
    ident = ' '.join(out)
    return f'<Object {ident}>'

lkr.Resource.__repr__ = _resource__repr__

def _resource__str__(self):
    return f'{self.apiVersion}/{self.kind}'

lkr.Resource.__str__ = _resource__str__


def get_resource(resource: lkr.Resource) -> lkr.Resource:
    """Ensure lightkube Resources known their own apiVersion and kind.
    See: https://github.com/gtsystem/lightkube/issues/76
    """
    info = lkr.api_info(resource)
    try:
        if resource.apiVersion in ('', None):
            resource.apiVersion = info.resource.api_version
        if resource.kind in ('', None):
            resource.kind = info.resource.kind
    except AttributeError as e:
        pass
    # For custom resources which are defined with a dataclass decorator
    # our above monkey patching is overwritten.
    # So patch resources again to ensure we always use our own __repr__.
    resource.__repr__ = _resource__repr__
    resource.__str__ = _resource__str__
    return resource


@dataclass
class GroupVersion:
    group: str
    version: str


@dataclass
class ObjectMeta(meta_v1.ObjectMeta):
    def __post_init__(self, **kwargs):
        # Set defaults for commonly used nested data structures.
        if self.annotations is None:
            self.annotations = {}
        if self.finalizers is None:
            self.finalizers = []
        if self.labels is None:
            self.labels = {}
        if self.managedFields is None:
            self.managedFields = []
        if self.ownerReferences is None:
            self.ownerReferences = []


class Resource:
    apiVersion: str = None
    # apiVersion: Annotated[str,
    #    'APIVersion defines the versioned schema of this representation of an object.'
    #    'Servers should convert recognized schemas to the latest internal value, and'
    #    'may reject unrecognized values.'
    #    'More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources'
    # ] = None
    kind: str = None
    # kind: Annotated[str,
    #     'Kind is a string value representing the REST resource this object represents.'
    #     'Servers may infer this from the endpoint the client submits requests to.'
    #     'Cannot be updated. In CamelCase.'
    #     'More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds'
    # ]
    metadata: ObjectMeta = None


class GenericStruct:
    # Threat me like a dict when generating schema.
    __schema_type__ = dict

    def __init__(self, *args, **kwargs):
        # print(f'GenericStruct.__init__ {args} {kwargs}')
        self.__dict__ = {}
        for k, v in kwargs.items():
            self.__dict__[k] = self.__deserialize(v)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = self.__deserialize(value)

    def __setattr__(self, key, value):
        if key == '__dict__':
            object.__setattr__(self, key, value)
        else:
            object.__setattr__(self, key, self.__deserialize(value))

    def __deserialize(self, field):
        if isinstance(field, dict):
            return GenericStruct(**{k: self.__deserialize(v) for k, v in field.items()})
        elif isinstance(field, (list, tuple)):
            return [self.__deserialize(item) for item in field]
        else:
            return field


class PartialObjectMetadata(GenericStruct):
    def __init__(self, name, namespace=None):
        if name is None:
            raise TypeError('PartialObjectMetadata: name can not be None')
        super().__init__(**{'metadata': {'name': name, 'namespace': namespace}})
