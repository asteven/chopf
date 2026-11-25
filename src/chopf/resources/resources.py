from dataclasses import dataclass

from lightkube.models import meta_v1

from . import identity


@dataclass
class GroupVersion:
    group: str
    version: str


class ObjectMeta(meta_v1.ObjectMeta):
    """ObjectMeta with usable defaults for commonly mutated nested structures.

    Under lightkube v1 this is a plain msgspec.Struct subclass; msgspec
    calls ``__post_init__`` (no arguments) on construction *and* on
    decode/convert, so decoded objects get the same defaults.
    """

    def __post_init__(self):
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
    """Base class for chopf's own resources (created via @resource).

    Provides readable dunders without touching lightkube's classes.
    ``apiVersion``/``kind``/``metadata`` are real msgspec fields on the
    generated resource classes (see resources/crd.py); they must NOT be
    declared here — plain class attributes would shadow the struct's
    member descriptors and break instance assignment.
    """

    def __repr__(self):
        return identity.resource_repr(self)

    def __str__(self):
        return identity.resource_str(self)


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
        super().__init__(metadata={'name': name, 'namespace': namespace})
