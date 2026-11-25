import dataclasses
import typing as t
from dataclasses import dataclass
from typing import dataclass_transform

import msgspec
from lightkube.core import resource as lkr
from lightkube.core.resource_registry import resource_registry
from lightkube.core.schema import DictMixin
from lightkube.models.apiextensions_v1 import (
    CustomResourceColumnDefinition,
    CustomResourceDefinitionNames,
    CustomResourceDefinitionSpec,
)
from lightkube.resources.apiextensions_v1 import (
    CustomResourceDefinition,
)

from . import Resource
from .registry import custom_resource_registry

_resource_verbs = [
    'delete',
    'deletecollection',
    'get',
    'global_list',
    'global_watch',
    'list',
    'patch',
    'post',
    'put',
    'watch',
]

_subresource_verbs = [
    'get',
    'patch',
    'put',
]


class ModelMixin(DictMixin):
    """Base class for chopf models and resources.

    Under lightkube v1 this is a msgspec.Struct (via DictMixin); decoding
    is always eager, so the old ``lazy`` argument is accepted for call-site
    compatibility but ignored.
    """

    @classmethod
    def from_dict(cls, d, lazy=False):
        if isinstance(d, cls):
            return d
        return super().from_dict(d)


def _struct_from_model(name, model, bases, extra_fields=()):
    """Create a real msgspec.Struct class from a user model.

    ``model`` may be a plain class, a stdlib dataclass, or an existing
    Struct. Stdlib dataclasses are only used as a *parse* step (convenient
    access to defaults/default_factories); the emitted class is always a
    Struct, which lightkube v1 requires for direct decoding of list/watch
    items.

    ``extra_fields`` are (name, type, default) tuples injected ahead of the
    model's own fields (used for apiVersion/kind identity); model fields
    with the same name are dropped.
    """
    injected = {f[0] for f in extra_fields}
    fields = list(extra_fields)
    namespace = {'__doc__': model.__doc__}

    if isinstance(model, type) and issubclass(model, msgspec.Struct):
        for f in msgspec.structs.fields(model):
            if f.name in injected:
                continue
            if f.required:
                fields.append((f.name, f.type))
            elif f.default_factory is not msgspec.NODEFAULT:
                fields.append(
                    (f.name, f.type, msgspec.field(default_factory=f.default_factory))
                )
            else:
                fields.append((f.name, f.type, f.default))
        field_names = {f.name for f in msgspec.structs.fields(model)}
    else:
        if not dataclasses.is_dataclass(model):
            model = dataclass(model, kw_only=True)
        type_hints = t.get_type_hints(model, include_extras=True)
        field_names = set()
        for f in dataclasses.fields(model):
            if f.name in injected:
                continue
            field_names.add(f.name)
            type_ = type_hints.get(f.name, t.Any)
            if f.default is not dataclasses.MISSING:
                fields.append((f.name, type_, f.default))
            elif f.default_factory is not dataclasses.MISSING:
                fields.append(
                    (f.name, type_, msgspec.field(default_factory=f.default_factory))
                )
            else:
                fields.append((f.name, type_))

    # Carry over user-defined methods/properties/class attributes.
    for attr, value in model.__dict__.items():
        if attr in field_names:
            continue
        if attr == '__post_init__' or not attr.startswith('__'):
            namespace[attr] = value

    return msgspec.defstruct(
        name,
        fields,
        bases=bases,
        kw_only=True,
        module=model.__module__,
        namespace=namespace,
    )


# @see https://www.brendanp.com/pretty-printing-with-kubebuilder/
def printcolumn(
    name, jsonpath, type='string', description=None, format=None, priority=None
):
    def _wrap(cls):
        crd_version = custom_resource_registry.get_crd_version(cls)
        if crd_version.additionalPrinterColumns is None:
            crd_version.additionalPrinterColumns = []
        column = CustomResourceColumnDefinition(
            name=name,
            type=type,
            jsonPath=jsonpath,
            description=description,
            format=format,
            priority=priority,
        )
        # Preserve the order.
        crd_version.additionalPrinterColumns.insert(0, column)
        return cls

    return _wrap


# @see https://mypy.readthedocs.io/en/stable/additional_features.html
@dataclass_transform()
def resource(
    group=None,
    version=None,
    kind=None,
    scope=None,
    singular=None,
    plural=None,
    short_names=None,
    served=True,
    storage=True,
):
    def _wrap(model):
        # The decorated class is our model.

        nonlocal \
            group, \
            version, \
            kind, \
            scope, \
            singular, \
            plural, \
            short_names, \
            served, \
            storage

        if not kind:
            kind = model.__name__
        if singular is None:
            singular = kind.lower()
        if plural is None:
            if singular[-1] == 's':
                plural = f'{singular}es'
            else:
                plural = f'{singular}s'

        # Identity fields default to None, exactly like lightkube's own
        # resources: the client fills them from _api_info on write
        # (prepare_request) and the server payload carries them on read.
        # Do NOT default them to the real values — DictMixin encodes with
        # omit_defaults=True, so default-equal values are stripped from
        # the payload and the API server rejects the object
        # ("Object 'Kind' is missing"). NB: never assign apiVersion/kind
        # as plain class attributes either — that would shadow the
        # struct's member descriptors and break instance assignment.
        identity_fields = [
            ('apiVersion', str, None),
            ('kind', str, None),
        ]

        # Create the resource struct based on the model.
        if scope == 'Cluster':
            bases = (Resource, lkr.GlobalResource, ModelMixin)
        else:
            bases = (Resource, lkr.NamespacedResourceG, ModelMixin)
        _Resource = _struct_from_model(kind, model, bases, extra_fields=identity_fields)
        # Create lightkube api-info.
        _Resource._api_info = lkr.ApiInfo(
            resource=lkr.ResourceDef(group, version, kind),
            plural=plural,
            verbs=_resource_verbs,
        )

        # Create and register the resource with our own registry.
        try:
            # Check if a crd already exists for this resource and reuse it.
            crd = custom_resource_registry.get_crd(_Resource)
        except custom_resource_registry.ResourceNotFoundError:
            # Otherwise create a new one and register it with the registry.
            crd = CustomResourceDefinition(
                apiVersion='apiextensions.k8s.io/v1',
                kind='CustomResourceDefinition',
                metadata={'name': f'{plural}.{group}'},
                spec=CustomResourceDefinitionSpec(
                    group=group,
                    names=CustomResourceDefinitionNames(
                        kind=kind,
                        listKind=f'{kind}List',
                        plural=plural,
                        singular=singular,
                        shortNames=short_names,
                    ),
                    scope=scope,
                    versions=[],
                ),
            )
            custom_resource_registry.add(crd)

        # Get and configure the version.
        crd_version = custom_resource_registry.get_crd_version(_Resource)
        crd_version.name = version
        crd_version.served = served
        crd_version.storage = storage

        # Configure this versions status subresource.
        status = t.get_type_hints(model).get('status', None)
        if status is not None and getattr(status, '__subresource', False):
            crd_version.subresources['status'] = {}
            status_resource_name = f'{kind}Status'
            # Create the status sub-resource struct based on the same model.
            if scope == 'Cluster':
                status_bases = (lkr.GlobalSubResource, ModelMixin)
            else:
                status_bases = (lkr.NamespacedSubResource, ModelMixin)
            _StatusResource = _struct_from_model(
                status_resource_name,
                model,
                status_bases,
                extra_fields=identity_fields,
            )
            _StatusResource._api_info = lkr.ApiInfo(
                resource=_Resource._api_info.resource,
                parent=_Resource._api_info.resource,
                plural=_Resource._api_info.plural,
                verbs=_subresource_verbs,
                action='status',
            )
            _Resource.Status = _StatusResource

        # Add the version to the crd.
        crd.spec.versions.append(crd_version)

        # Register the crd, version and resource class in the registry.
        # This is later used to generate a schema which can be added
        # to kubernetes.
        custom_resource_registry.register_resource(crd, crd_version, _Resource)

        # Register the resource with lightkube.
        # TODO: removed this in favour of crd_observer in manager.
        # resource_registry.register(_Resource)
        _ = resource_registry

        # Return the resource in place of the decorated class.
        return _Resource

    return _wrap


@dataclass_transform()
def subresource(resource_class=None, /):
    def _wrap(cls):
        # Create a model struct based on the decorated class.
        model = _struct_from_model(cls.__name__, cls, (ModelMixin,))
        model.__subresource = True
        return model

    if resource_class is None:
        return _wrap
    else:
        return _wrap(resource_class)


@dataclass_transform()
def model(resource_class=None, /):
    def _wrap(cls):
        # Create a model struct based on the decorated class.
        return _struct_from_model(cls.__name__, cls, (ModelMixin,))

    if resource_class is None:
        return _wrap
    else:
        return _wrap(resource_class)
