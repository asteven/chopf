"""
Some code in this file is based on
https://github.com/Peter554/dc_schema

MIT License

Copyright (c) 2022 Peter Byfield



Resources:

https://www.brendanp.com/pretty-printing-with-kubebuilder/

https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/

- Kubernetes JSONSchemaProps
    https://dev-k8sref-io.web.app/docs/common-definitions/jsonschemaprops-/

"""

from __future__ import annotations


import builtins
import datetime
import enum
import dataclasses
import numbers
import inspect
import types
import typing as t


from lightkube.models.util_intstr import IntOrString


_MISSING = dataclasses.MISSING


def get_schema(dc):
    # return _GetSchema()(dc)
    schema = _GetSchema()(dc)
    definitions = None
    if 'definitions' in schema:
        definitions = schema.pop('definitions')
    elif '$defs' in schema:
        definitions = schema.pop('$defs')
    if definitions:
        #if 'ObjectMeta' in definitions:
        #    # Don't need/want detailed ObjectMeta schema in crd.
        #    definitions['ObjectMeta'] = {'type': 'object'}
            #definitions['ObjectMeta'] = {
            #    'type': 'object',
            #    'title': 'ObjectMeta',
            #    'properties': {
            #        'name': {'type': 'string'},
            #        'generateName': {'type': 'string'},
            #    },
            #}
        # First dereference any nested definitions.
        # This is just a performance optimisation so we only dereference
        # each models once instead of repeating that for each reference.
        _dereference_schema(definitions, definitions)
        # Then dereference the actual schema.
        _dereference_schema(schema, definitions)
        # Then cleanup the schema into something that kubernetes agrees with.
        # TODO: is this still needed?
        _clean_schema(schema)

    # We are not allowed to specify a detailed ObjectMeta in crd.
    schema['properties']['metadata'] = {'type': 'object'}

    return schema


def _resolve_definition(name, definitions):
    ref_name = name.rpartition('/')[-1]
    definition = definitions[ref_name]
    return definition


def _dereference_schema(schema, definitions, parent=None, key=None):
    """Find and dereference objects in the given schema.

    '#/definitions/myElement' -> schema.definitions[myElement]
    """
    if hasattr(schema, 'items'):
        if list(schema.keys()) == ['$ref']:
            definition = _resolve_definition(schema['$ref'], definitions)
            schema.clear()
            schema.update(definition.items())
        else:
            for k, v in schema.items():
                if k == '$ref':
                    # print(f'{type(parent)} {key}: {k} -> {v}')
                    definition = _resolve_definition(v, definitions)
                    if isinstance(parent, dict):
                        parent[key] = definition
                    elif isinstance(parent, list):
                        parent[parent.index(key)] = definition
                    v = definition
                if isinstance(v, dict):
                    _dereference_schema(v, definitions, parent=schema, key=k)
                elif isinstance(v, list):
                    for i, d in enumerate(v):
                        _dereference_schema(d, definitions, parent=v, key=d)
                else:
                    # print(f'unhandled k: {k}; v: {v}; %s' % type(v))
                    pass


def _clean_schema(schema):
    """Clean the schema for use with kubernetes.

    Pydantic uses allOf to preserve some fields like title and description
    that would otherwise be overwritten by nested models.
    Kubernetes does not like that so we work around that by merging
    the nested model properties.

    We basically turn this:

    ```
    {
       'tokenSecretRef': {
          'title': 'Tokensecretref',
          'description': 'Some interesting field description.',
          'allOf': [{
             'title': 'SecretRef',
             'description': 'Some model docstring.',
             'type': 'object',
             'properties': {
                'name': {'title': 'Name', 'type': 'string'}, 'key': {'title': 'Key', 'type': 'string'}
             },
             'required': ['name']
          }]
       }
    }
    ```

    into this:

    ```
    {
       'tokenSecretRef': {
          'title': 'Tokensecretref',
          'description': 'Some interesting field description.',
          'type': 'object',
          'properties': {
             'name': {'title': 'Name', 'type': 'string'}, 'key': {'title': 'Key', 'type': 'string'}
          },
          required': ['name']
       }
    }
    ```
    """
    # print(f'### _clean_schema: {schema}')
    if hasattr(schema, 'items'):
        if 'allOf' in schema:
            # print(f'### _clean_schema allOf detected: {schema}')
            value = schema['allOf']
            # print(f'### _clean_schema value: {value}')
            if len(value) == 1:
                child = value[0]
                for k, v in child.items():
                    schema.setdefault(k, v)
                schema.pop('allOf')
            _clean_schema(schema)
        else:
            if isinstance(schema, dict):
                for k, v in schema.items():
                    _clean_schema(v)
            elif isinstance(schema, list):
                for i, d in enumerate(schema):
                    _clean_schema(d)


_Format = t.Literal[
    'date-time',
    'time',
    'date',
    'duration',
    'email',
    'idn-email',
    'hostname',
    'idn-hostname',
    'ipv4',
    'ipv6',
    'uuid',
    'uri',
    'uri-reference',
    'iri',
    'iri-reference',
]


@dataclasses.dataclass(frozen=True)
class SchemaAnnotation:
    title: t.Optional[str] = None
    description: t.Optional[str] = None
    examples: t.Optional[list[t.Any]] = None
    deprecated: t.Optional[bool] = None
    min_length: t.Optional[int] = None
    max_length: t.Optional[int] = None
    pattern: t.Optional[str] = None
    format: t.Optional[_Format] = None
    minimum: t.Optional[numbers.Number] = None
    maximum: t.Optional[numbers.Number] = None
    exclusive_minimum: t.Optional[numbers.Number] = None
    exclusive_maximum: t.Optional[numbers.Number] = None
    multiple_of: t.Optional[numbers.Number] = None
    min_items: t.Optional[int] = None
    max_items: t.Optional[int] = None
    unique_items: t.Optional[bool] = None

    def schema(self):
        key_map = {
            'min_length': 'minLength',
            'max_length': 'maxLength',
            'exclusive_minimum': 'exclusiveMinimum',
            'exclusive_maximum': 'exclusiveMaximum',
            'multiple_of': 'multipleOf',
            'min_items': 'minItems',
            'max_items': 'maxItems',
            'unique_items': 'uniqueItems',
        }
        return {
            key_map.get(k, k): v
            for k, v in dataclasses.asdict(self).items()
            if v is not None
        }


class _GetSchema:
    def __call__(self, dc):
        self.root = dc
        self.seen_root = False

        self.defs = {}
        schema = self.get_dc_schema(dc, SchemaAnnotation())
        if self.defs:
            schema['$defs'] = self.defs

        return {
            # "$schema": "https://json-schema.org/draft/2020-12/schema",
            **schema,
        }

    def get_dc_schema(self, dc, annotation):
        if dc == self.root:
            if self.seen_root:
                return {'allOf': [{'$ref': '#'}], **annotation.schema()}
            else:
                self.seen_root = True
                schema = self.create_dc_schema(dc)
                return schema
        else:
            if dc.__name__ not in self.defs:
                schema = self.create_dc_schema(dc)
                self.defs[dc.__name__] = schema
            return {
                'allOf': [{'$ref': f'#/$defs/{dc.__name__}'}],
                **annotation.schema(),
            }

    def has_custom_doc_string(self, dc):
        # see python dataclasses.py
        if not dc.__doc__:
            return False
        try:
            text_sig = str(inspect.signature(dc)).replace(' -> None', '')
        except (TypeError, ValueError):
            text_sig = ''
        return dc.__doc__ != (dc.__name__ + text_sig)

    def create_dc_schema(self, dc):
        if hasattr(dc, 'SchemaConfig'):
            annotation = getattr(dc.SchemaConfig, 'annotation', SchemaAnnotation())
        else:
            # Do not use docstrings for lightkube provided models as they are to large.
            if dc.__module__.startswith('lightkube'):
                annotation = SchemaAnnotation()
            else:
                # Use the dataclasses doc-string as description, but only if it was not generated
                # by the @dataclass decorator.
                if self.has_custom_doc_string(dc):
                    # Strip unwanted leading and trailing whitespace.
                    lines = dc.__doc__.splitlines()
                    data = []
                    for line in lines:
                        line = line.rstrip()
                        line = line.removeprefix('    ')
                        data.append(line)
                    description = '\n'.join(data)
                    annotation = SchemaAnnotation(description=description)
                else:
                    annotation = SchemaAnnotation()
        schema = {
            'type': 'object',
            'title': dc.__name__,
            **annotation.schema(),
            'properties': {},
            'required': [],
        }
        type_hints = t.get_type_hints(dc, include_extras=True)
        for field in dataclasses.fields(dc):
            type_ = type_hints[field.name]
            # origin = t.get_origin(type_)
            # print(f'{field.name}, {type_}, {origin}, {field.default}', file=sys.stderr)
            schema['properties'][field.name] = self.get_field_schema(
                type_, field.default, SchemaAnnotation()
            )
            field_is_optional = (
                field.default is not _MISSING or field.default_factory is not _MISSING
            )
            if not field_is_optional:
                schema['required'].append(field.name)
        if not schema['required']:
            schema.pop('required')
        return schema

    def get_field_schema(self, type_, default, annotation):
        origin = t.get_origin(type_)
        # print(f'get_field_schema: {type_}, {origin}, {default}, {annotation}', file=sys.stderr)
        if dataclasses.is_dataclass(type_):
            return self.get_dc_schema(type_, annotation)

        if type_ == IntOrString:
            # IntOrString should always be treated as int when generating schema.
            return self.get_int_schema(default, annotation)

        if origin is not None:
            field_type = origin
        else:
            if hasattr(type_, '__schema_type__'):
                field_type = type_.__schema_type__
            else:
                field_type = type_

        match field_type:
            case t.Union | types.UnionType:
                return self.get_union_schema(type_, default, annotation)
            case t.Literal:
                return self.get_literal_schema(type_, default, annotation)
            case t.Annotated:
                return self.get_annotated_schema(type_, default)
            case builtins.dict:
                return self.get_dict_schema(type_, annotation)
            case builtins.list:
                return self.get_list_schema(type_, annotation)
            case builtins.tuple:
                return self.get_tuple_schema(type_, default, annotation)
            case builtins.set:
                return self.get_set_schema(type_, annotation)
            case types.NoneType:
                return self.get_none_schema(default, annotation)
            case builtins.str | _ if issubclass(field_type, str):
                return self.get_str_schema(default, annotation)
            case builtins.bool | _ if issubclass(field_type, bool):
                return self.get_bool_schema(default, annotation)
            case builtins.int | _ if issubclass(field_type, int):
                return self.get_int_schema(default, annotation)
            case _ if issubclass(field_type, numbers.Number):
                return self.get_number_schema(default, annotation)
            case _ if issubclass(field_type, enum.Enum):
                return self.get_enum_schema(type_, default, annotation)
            case _ if issubclass(field_type, datetime.datetime):
                return self.get_datetime_schema(annotation)
            case _ if issubclass(field_type, datetime.date):
                return self.get_date_schema(annotation)
            case _:
                raise NotImplementedError(f"field type '{type_}' not implemented")

    def get_union_schema(self, type_, default, annotation):
        args = t.get_args(type_)
        # print(f'get_union_schema: {type_}, {args}', file=sys.stderr)
        schema = {}
        # typing.Optional is treated as typing.Union[_type, None].
        # But we don't whant that union in our schema for this case so
        # we convert the union back to a single type here.
        if default is not _MISSING:
            args = list(args)
            args.remove(types.NoneType)
        if default not in (_MISSING, None):
            schema['default'] = default
        if len(args) > 1:
            schema.update(
                {
                    'anyOf': [
                        self.get_field_schema(arg, _MISSING, SchemaAnnotation())
                        for arg in args
                    ],
                    **annotation.schema(),
                }
            )
        else:
            schema = self.get_field_schema(args[0], _MISSING, SchemaAnnotation())
        return schema

    def get_literal_schema(self, type_, default, annotation):
        if default in (_MISSING, None):
            schema = {**annotation.schema()}
        else:
            schema = {'default': default, **annotation.schema()}
        args = t.get_args(type_)
        return {'enum': list(args), **schema}

    def get_dict_schema(self, type_, annotation):
        args = t.get_args(type_)
        assert len(args) in (0, 2)
        if args:
            assert isinstance(args[0], str)
            return {
                'type': 'object',
                'additionalProperties': self.get_field_schema(
                    args[1], _MISSING, SchemaAnnotation()
                ),
                **annotation.schema(),
            }
        else:
            return {'type': 'object', **annotation.schema()}

    def get_list_schema(self, type_, annotation):
        args = t.get_args(type_)
        assert len(args) in (0, 1)
        if args:
            return {
                'type': 'array',
                'items': self.get_field_schema(args[0], _MISSING, SchemaAnnotation()),
                **annotation.schema(),
            }
        else:
            return {'type': 'array', **annotation.schema()}

    def get_tuple_schema(self, type_, default, annotation):
        if default in (_MISSING, None):
            schema = {**annotation.schema()}
        else:
            schema = {'default': list(default), **annotation.schema()}
        args = t.get_args(type_)
        if args and len(args) == 2 and args[1] is ...:
            schema = {
                'type': 'array',
                'items': self.get_field_schema(args[0], _MISSING, SchemaAnnotation()),
                **schema,
            }
        elif args:
            schema = {
                'type': 'array',
                'prefixItems': [
                    self.get_field_schema(arg, _MISSING, SchemaAnnotation())
                    for arg in args
                ],
                'minItems': len(args),
                'maxItems': len(args),
                **schema,
            }
        else:
            schema = {'type': 'array', **schema}
        return schema

    def get_set_schema(self, type_, annotation):
        args = t.get_args(type_)
        assert len(args) in (0, 1)
        if args:
            return {
                'type': 'array',
                'items': self.get_field_schema(args[0], _MISSING, SchemaAnnotation()),
                'uniqueItems': True,
                **annotation.schema(),
            }
        else:
            return {'type': 'array', 'uniqueItems': True, **annotation.schema()}

    def get_none_schema(self, default, annotation):
        if default in (_MISSING, None):
            return {'type': 'null', **annotation.schema()}
        else:
            return {'type': 'null', 'default': default, **annotation.schema()}

    def get_str_schema(self, default, annotation):
        if default in (_MISSING, None):
            return {'type': 'string', **annotation.schema()}
        else:
            return {'type': 'string', 'default': default, **annotation.schema()}

    def get_bool_schema(self, default, annotation):
        if default in (_MISSING, None):
            return {'type': 'boolean', **annotation.schema()}
        else:
            return {'type': 'boolean', 'default': default, **annotation.schema()}

    def get_int_schema(self, default, annotation):
        if default in (_MISSING, None):
            return {'type': 'integer', **annotation.schema()}
        else:
            return {'type': 'integer', 'default': default, **annotation.schema()}

    def get_number_schema(self, default, annotation):
        if default in (_MISSING, None):
            return {'type': 'number', **annotation.schema()}
        else:
            return {'type': 'number', 'default': default, **annotation.schema()}

    def get_enum_schema(self, type_, default, annotation):
        if type_.__name__ not in self.defs:
            self.defs[type_.__name__] = {
                'title': type_.__name__,
                'enum': [v.value for v in type_],
            }
        if default in (_MISSING, None):
            return {
                'allOf': [{'$ref': f'#/$defs/{type_.__name__}'}],
                **annotation.schema(),
            }
        else:
            return {
                'allOf': [{'$ref': f'#/$defs/{type_.__name__}'}],
                'default': default.value,
                **annotation.schema(),
            }

    def get_annotated_schema(self, type_, default):
        args = t.get_args(type_)
        assert len(args) == 2
        annotation = SchemaAnnotation(description=args[1])
        return self.get_field_schema(args[0], default, annotation)

    def get_datetime_schema(self, annotation):
        return {'type': 'string', 'format': 'date-time', **annotation.schema()}

    def get_date_schema(self, annotation):
        return {'type': 'string', 'format': 'date', **annotation.schema()}
