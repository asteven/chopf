import dataclasses

from dataclasses import dataclass
from typing import dataclass_transform

from lightkube.core.schema import DictMixin
from lightkube.core import resource as lkr
from lightkube.core.resource_registry import resource_registry

from lightkube.resources.apiextensions_v1 import (
    CustomResourceDefinition,
)

from lightkube.models.apiextensions_v1 import (
    CustomResourceDefinitionNames,
    CustomResourceDefinitionSpec,
    CustomResourceColumnDefinition,
)


from .registry import custom_resource_registry
from . import Resource


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
    @classmethod
    def from_dict(cls, d, lazy=True):
        # Custom Resource models can not be lazy.
        lazy = False
        if isinstance(d, cls):
            return d
        else:
            return super(ModelMixin, cls).from_dict(d, lazy=lazy)

    def to_dict(self, dict_factory=dict):
        d = super().to_dict(dict_factory=dict_factory)
        return d


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

        # Ensure the model is a dataclass.
        if not dataclasses.is_dataclass(model):
            model = dataclass(model, kw_only=True)

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

        # Ensure class __dict__ does not contain a nested __dict__ key.
        # See https://jira.mongodb.org/browse/MOTOR-460
        model_dict = dict(model.__dict__)
        model_dict.pop('__dict__', None)

        # Create a lightkube resource based on the model.
        if scope == 'Cluster':
            _Resource = type(
                kind,
                (Resource, lkr.GlobalResource, ModelMixin),
                model_dict,
            )
        else:
            _Resource = type(
                kind,
                (Resource, lkr.NamespacedResourceG, ModelMixin),
                model_dict,
            )
        # Create lightkube api-info.
        _Resource._api_info = lkr.ApiInfo(
            resource=lkr.ResourceDef(group, version, kind),
            plural=plural,
            verbs=_resource_verbs,
        )
        # Ensure our resource knows what it is.
        _Resource.apiVersion = _Resource._api_info.resource.api_version
        _Resource.kind = _Resource._api_info.resource.kind

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
        crd_version = custom_resource_registry.get_crd_version(model)
        crd_version.name = version
        crd_version.served = served
        crd_version.storage = storage

        # Configure this versions status subresource.
        status = model.__annotations__.get('status', None)
        if status and status.__subresource:
            crd_version.subresources['status'] = {}
            status_resource_name = f'{kind}Status'
            # Create lightkube sub-resource based on the decorated class.
            if scope == 'Cluster':
                _StatusResource = type(
                    status_resource_name,
                    (
                        lkr.GlobalSubResource,
                        ModelMixin,
                    ),
                    model_dict,
                )
            else:
                _StatusResource = type(
                    status_resource_name,
                    (
                        lkr.NamespacedSubResource,
                        ModelMixin,
                    ),
                    model_dict,
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
        #resource_registry.register(_Resource)

        # Return the resource in place of the decorated class.
        return _Resource

    return _wrap


@dataclass_transform()
def subresource(resource_class=None, /):
    def _wrap(cls):
        # Ensure class __dict__ does not contain a nested __dict__ key.
        #   see https://jira.mongodb.org/browse/MOTOR-460
        cls_dict = dict(cls.__dict__)
        cls_dict.pop('__dict__', None)

        # Create a model based on the decorated class.
        model = type(cls.__name__, (ModelMixin,), cls_dict)
        model.__subresource = True

        # Ensure the class is a dataclass.
        if not dataclasses.is_dataclass(model):
            model = dataclass(model)
        return model

    if resource_class is None:
        return _wrap
    else:
        return _wrap(resource_class)


@dataclass_transform()
def model(resource_class=None, /):
    def _wrap(cls):
        # Ensure class __dict__ does not contain a nested __dict__ key.
        cls_dict = dict(cls.__dict__)
        cls_dict.pop('__dict__', None)
        # Create a model based on the decorated class.
        model = type(cls.__name__, (ModelMixin,), cls_dict)

        # Ensure the class is a dataclass.
        if not dataclasses.is_dataclass(model):
            model = dataclass(model)
        return model

    if resource_class is None:
        return _wrap
    else:
        return _wrap(resource_class)
