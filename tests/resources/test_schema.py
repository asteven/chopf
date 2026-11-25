"""Characterization tests for openAPIV3Schema generation (schema.py).

Seam under test: get_schema(dc) — the function CRD generation relies on.
These pin current behavior so CRD/schema work can be refactored safely.
"""

import datetime
import enum
from dataclasses import dataclass, field
from typing import Annotated, Literal

from lightkube.models.util_intstr import IntOrString

from chopf import crd
from chopf.resources import GenericStruct, ObjectMeta
from chopf.resources.schema import get_schema


@dataclass
class Inner:
    """An inner model."""

    x: int
    y: str = 'a'


@dataclass
class Outer:
    name: str
    inner: Inner
    count: int = 3


class Color(enum.Enum):
    RED = 'red'
    BLUE = 'blue'


def prop(schema, name):
    return schema['properties'][name]


def test_simple_fields():
    schema = get_schema(Outer)

    assert schema['type'] == 'object'
    assert schema['title'] == 'Outer'
    assert schema['required'] == ['name', 'inner']
    assert prop(schema, 'name') == {'type': 'string'}
    assert prop(schema, 'count') == {'type': 'integer', 'default': 3}


def test_nested_dataclass_is_inlined():
    schema = get_schema(Outer)

    inner = prop(schema, 'inner')
    assert inner['type'] == 'object'
    assert inner['title'] == 'Inner'
    # Docstring becomes the description for non-lightkube models.
    assert inner['description'] == 'An inner model.'
    assert inner['properties']['x'] == {'type': 'integer'}
    assert inner['properties']['y'] == {'type': 'string', 'default': 'a'}
    assert inner['required'] == ['x']
    # Definitions are dereferenced and removed.
    assert '$defs' not in schema
    assert 'definitions' not in schema


def test_metadata_property_is_reduced_to_plain_object():
    schema = get_schema(Outer)

    # get_schema always rewrites metadata, even if not present as field.
    @dataclass
    class WithMeta:
        metadata: ObjectMeta
        spec: dict = None

    schema = get_schema(WithMeta)
    assert prop(schema, 'metadata') == {'type': 'object'}


def test_field_types():
    @dataclass
    class Types:
        s: str = ''
        b: bool = False
        i: int = 0
        f: float = 0.0
        maybe: int | None = None
        lit: Literal['a', 'b'] = 'a'
        dt: datetime.datetime = None
        d: datetime.date = None
        items: list[int] = field(default_factory=list)
        mapping: dict[str, int] = field(default_factory=dict)
        tags: set[str] = field(default_factory=set)
        pair: tuple[int, str] = (0, '')
        color: Color = Color.RED
        quantity: IntOrString = 0

    schema = get_schema(Types)
    props = schema['properties']

    assert props['s'] == {'type': 'string', 'default': ''}
    assert props['b'] == {'type': 'boolean', 'default': False}
    assert props['i'] == {'type': 'integer', 'default': 0}
    assert props['f'] == {'type': 'number', 'default': 0.0}
    assert props['maybe'] == {'type': 'integer'}
    assert props['lit'] == {'enum': ['a', 'b'], 'default': 'a'}
    assert props['dt'] == {'type': 'string', 'format': 'date-time'}
    assert props['d'] == {'type': 'string', 'format': 'date'}
    assert props['items'] == {'type': 'array', 'items': {'type': 'integer'}}
    assert props['mapping'] == {
        'type': 'object',
        'additionalProperties': {'type': 'integer'},
    }
    assert props['tags'] == {
        'type': 'array',
        'items': {'type': 'string'},
        'uniqueItems': True,
    }
    assert props['pair']['type'] == 'array'
    assert props['pair']['prefixItems'] == [{'type': 'integer'}, {'type': 'string'}]
    # msgspec.json.schema emits enum values sorted; order is irrelevant
    # to kubernetes.
    assert props['color']['enum'] == ['blue', 'red']
    # IntOrString is always treated as int.
    assert props['quantity'] == {'type': 'integer', 'default': 0}
    # Everything has a default: nothing is required.
    assert 'required' not in schema


def test_annotated_description():
    @dataclass
    class AnnotatedModel:
        lifetime: Annotated[int, 'pod lifetime in seconds'] = 20

    schema = get_schema(AnnotatedModel)
    assert prop(schema, 'lifetime') == {
        'type': 'integer',
        'default': 20,
        'description': 'pod lifetime in seconds',
    }


def test_schema_config_annotation():
    @dataclass
    class Configured:
        x: int = 0

        class SchemaConfig:
            from chopf.resources.schema import SchemaAnnotation

            annotation = SchemaAnnotation(description='custom description')

    schema = get_schema(Configured)
    assert schema['description'] == 'custom description'


def test_generic_struct_is_plain_object():
    @dataclass
    class WithStruct:
        data: GenericStruct = None

    schema = get_schema(WithStruct)
    assert prop(schema, 'data') == {'type': 'object'}


def test_resource_end_to_end():
    @crd.resource(group='schema-tests.chopf', version='v1', scope='Namespaced')
    class Thing:
        """A test thing."""

        metadata: ObjectMeta
        spec: Inner

    schema = get_schema(Thing)

    assert schema['description'] == 'A test thing.'
    assert prop(schema, 'metadata') == {'type': 'object'}
    spec = prop(schema, 'spec')
    assert spec['type'] == 'object'
    assert spec['properties']['x'] == {'type': 'integer'}
    assert schema['required'] == ['metadata', 'spec']
