from typing import Union

from lightkube.resources.apiextensions_v1 import (
    CustomResourceDefinition,
)

from lightkube.models.apiextensions_v1 import (
    CustomResourceDefinitionVersion,
)

from lightkube.core.resource import api_info

from .schema import get_schema


class ResourceNotFoundError(Exception):
    pass


class ResourceVersionNotFoundError(Exception):
    pass


class CustomResourceRegistry:
    _crds = {}
    _versions = {}
    _resources = {}

    ResourceNotFoundError = ResourceNotFoundError
    ResourceVersionNotFoundError = ResourceVersionNotFoundError

    @classmethod
    def add(cls, crd: CustomResourceDefinition):
        name = getattr(crd.metadata, 'name', crd.metadata.get('name', None))
        cls._crds[name] = crd

    @classmethod
    def register_resource(
        cls,
        crd: CustomResourceDefinition,
        version: CustomResourceDefinitionVersion,
        resource_class: type,
    ):
        crd_name = getattr(crd.metadata, 'name', crd.metadata.get('name', None))
        if crd_name not in cls._resources:
            cls._resources[crd_name] = {}
        cls._resources[crd_name][version.name] = resource_class

    @classmethod
    def get_crd_version(cls, resource_class: type) -> CustomResourceDefinitionVersion:
        # As a resource class represents a specific version of a resource
        # we store them keyed by the resource class itself.
        key = f'{resource_class.__module__}.{resource_class.__name__}'
        try:
            crd_version = cls._versions[key]
        except KeyError:
            crd_version = CustomResourceDefinitionVersion(
                name=None,
                served=None,
                storage=None,
                additionalPrinterColumns=[],
                subresources={},
            )
            cls._versions[key] = crd_version
        return crd_version

    @classmethod
    def get_crd(
        cls, resource_class_or_name: Union[str, type]
    ) -> CustomResourceDefinition:
        if isinstance(resource_class_or_name, str):
            name = resource_class_or_name
        else:
            info = api_info(resource_class_or_name)
            name = f'{info.plural}.{info.resource.group}'
        try:
            return cls._crds[name]
        except KeyError as e:
            msg = f'Could not find custom resource definiton for: {name}'
            raise ResourceNotFoundError(msg) from e

    @classmethod
    def all_crds(cls) -> list[CustomResourceDefinition]:
        crds = []
        for name, crd in cls._crds.items():
            for version in crd.spec.versions:
                resource = cls._resources[name][version.name]
                schema = get_schema(resource)
                version.schema = {'openAPIV3Schema': schema}
            crds.append(crd)
        return crds
