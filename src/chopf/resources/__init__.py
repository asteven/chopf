import yaml

from .registry import (
    custom_resource_registry,
)
from .resources import (
    GenericStruct,
    GroupVersion,
    ObjectMeta,
    PartialObjectMetadata,
    Resource,
)

__all__ = [
    'GenericStruct',
    'GroupVersion',
    'ObjectMeta',
    'PartialObjectMetadata',
    'Resource',
    'crd',
    'custom_resource_registry',
]


def is_same_version(o1, o2):
    o1_resource_version = o1.metadata.resourceVersion
    o2_resource_version = o2.metadata.resourceVersion
    return (
        o1_resource_version is not None and o1_resource_version == o2_resource_version
    )


_resource_key_order = ['apiVersion', 'kind', 'metadata', 'spec', 'status']


def _drop_empty_values(value):
    """Recursively drop keys with empty values (None, []).

    Empty dicts are kept — they can be semantic in kubernetes manifests
    (e.g. ``subresources: {status: {}}`` enables the status subresource).
    False and 0 are kept for the same reason (e.g. ``served: false``).
    """
    if isinstance(value, dict):
        return {
            k: _drop_empty_values(v)
            for k, v in value.items()
            if v is not None and v != []
        }
    if isinstance(value, list):
        return [_drop_empty_values(v) for v in value]
    return value


def _resources_as_dicts(*objects):
    """Convert a list of given resources to dicts.
    - we omit keys that do not have a value.
    - we ensure the keys are emitted in a user readable order
    """
    dicts = []
    for obj in objects:
        if hasattr(obj, 'to_dict'):
            tmp = obj.to_dict()
        else:
            # dict-based generic resources and plain dicts
            tmp = dict(obj)
        tmp = _drop_empty_values(tmp)
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
