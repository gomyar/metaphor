
from metaphor.schema import Schema, CalcField


def serialize_spec(spec, admin=False):
    return {
        'name': spec.name,
        'fields': {
            name: serialize_field(f, admin) for name, f in spec.fields.items()
        },
    }

def serialize_schema(schema, admin=False):
    root_spec = {
        "name": "root",
        "fields": {
            name: serialize_field(root, admin) for name, root in schema.root.fields.items()
        },
        "type": "resource",
    }
    root_spec["fields"]["ego"] = {
        'name': "ego",
        'type': "ego",
        'target_spec_name': "user",
        'is_collection': False,
    }
    specs = {
        name: serialize_spec(spec, admin) for name, spec in schema.specs.items()
    }
    specs['root'] = root_spec
    return {
        'id': str(schema._id),
        'name': schema.name,
        'description': schema.description,
        'specs': specs,
        'root': root_spec,
        'version': schema.version,
        'current': schema.current,
        'groups': schema.groups,
    }


def serialize_field(field, admin=False):
    serialized = {
        'name': field.name,
        'type': field.field_type,
        'target_spec_name': field.target_spec_name,
        'is_collection': field.is_collection(),
    }
    if admin:
        serialized.update({
            'required': field.required,
            'indexed': field.indexed,
            'unique': field.unique,
            'unique_global': field.unique_global,
            'default': field.default,
        })
    if type(field) is CalcField:
        calc_type = field.infer_type()
        if not calc_type.is_primitive():
            serialized['target_spec_name'] = calc_type.name
        serialized['calc_str'] = field.calc_str
    return serialized


