from dataclasses import dataclass

from lightkube.models import meta_v1

from . import identity


@dataclass
class GroupVersion:
    group: str
    version: str


#: ObjectMeta fields normalized to usable empty defaults, mapped to a
#: factory for their empty value. ``managedFields`` is deliberately
#: excluded: operators never mutate it and it is often the largest field
#: on an object, so materializing it would be pure waste.
NORMALIZED_METADATA_FIELDS = {
    'annotations': dict,
    'labels': dict,
    'finalizers': list,
    'ownerReferences': list,
}


def normalize_metadata(obj):
    """Give any resource usable defaults for commonly used ObjectMeta fields.

    lightkube's built-in models decode absent collections as ``None``,
    which forces operator authors to guard e.g. ``obj.metadata.annotations``
    before every use. chopf's own :class:`ObjectMeta` already defaults these
    fields via ``__post_init__``; this function applies the same sugar to
    any resource, regardless of model class.

    Note that the object no longer faithfully mirrors server state: the
    normalized fields are re-encoded as empty maps/lists on update. That
    is benign — kubernetes treats empty maps as absent — and matches the
    semantics chopf's ObjectMeta has always had.
    """
    metadata = getattr(obj, 'metadata', None)
    if metadata is None:
        return obj
    if isinstance(metadata, dict):
        # Dict-backed generic resources.
        for field, default_factory in NORMALIZED_METADATA_FIELDS.items():
            if metadata.get(field) is None:
                metadata[field] = default_factory()
        return obj
    for field, default_factory in NORMALIZED_METADATA_FIELDS.items():
        if getattr(metadata, field, None) is None:
            setattr(metadata, field, default_factory())
    return obj


class ObjectMeta(meta_v1.ObjectMeta):
    """ObjectMeta with usable defaults for commonly mutated nested structures.

    Under lightkube v1 this is a plain msgspec.Struct subclass; msgspec
    calls ``__post_init__`` (no arguments) on construction *and* on
    decode/convert, so decoded objects get the same defaults.
    """

    def __post_init__(self):
        # Set defaults for commonly used nested data structures.
        for field, default_factory in NORMALIZED_METADATA_FIELDS.items():
            if getattr(self, field) is None:
                setattr(self, field, default_factory())


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
