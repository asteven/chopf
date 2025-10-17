import dataclasses
import yaml

from .registry import (
    custom_resource_registry,
)

from .resources import (
    get_resource,
    GenericStruct,
    GroupVersion,
    ObjectMeta,
    Resource,
    PartialObjectMetadata,
)

__all__ = [
    'crd',
    'custom_resource_registry',
    'GenericStruct',
    'get_resource',
    'GroupVersion',
    'ObjectMeta',
    'PartialObjectMetadata',
    'Resource',
]


def is_same_version(o1, o2):
    o1_resource_version = o1.metadata.resourceVersion
    o2_resource_version = o2.metadata.resourceVersion
    return (
        o1_resource_version is not None and o1_resource_version == o2_resource_version
    )


def _no_empty_value_dict(items, **kwargs):
    """Helper function that returns dicts
    that don't have empty values.
    """
    return dict(
        [(k, v) for k, v in items if v], **{k: v for k, v in kwargs.items() if v}
    )


_resource_key_order = ['apiVersion', 'kind', 'metadata', 'spec', 'status']


def _resources_as_dicts(*objects):
    """Convert a list of given resources to dicts.
    - we omit keys that do not have a value.
    - we ensure the keys are emitted in a user readable order
    """
    dicts = []
    for obj in objects:
        tmp = dataclasses.asdict(obj, dict_factory=_no_empty_value_dict)
        keys = dict.fromkeys(_resource_key_order + list(tmp.keys()))
        dicts.append({k: tmp[k] for k in keys if k in tmp})
    return dicts


class YamlDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def _str_presenter(dumper, data):
    """
    Preserve multiline strings when dumping yaml.
    https://github.com/yaml/pyyaml/issues/240
    """
    if '\n' in data:
        # Remove trailing spaces messing out the output.
        block = '\n'.join([line.rstrip() for line in data.splitlines()])
        if data.endswith('\n'):
            block += '\n'
        return dumper.represent_scalar('tag:yaml.org,2002:str', block, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


yaml.add_representer(str, _str_presenter, Dumper=YamlDumper)


def resources_to_yaml(*objects):
    """Helper function to serialize one or more CRD's to a yaml document
    that kubernetes understands.

    We prevent the yaml Dumper from using any alias references as
    kubernetes does not understand those.
    """
    dicts = _resources_as_dicts(*objects)
    return yaml.dump_all(dicts, sort_keys=False, Dumper=YamlDumper)


def all_crds(include_private=False):
    """Create and return CustomResourceDefinition's for all known and loaded resource classes."""
    return custom_resource_registry.all_crds()
