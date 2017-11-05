
import os
from bson.objectid import ObjectId


class Schema(object):
    def __init__(self, db, version):
        self.db = db
        self.version = version
        self.specs = {}
        self.add_resource_spec(ResourceSpec('root'))

    def __repr__(self):
        return "<Schema %s>" % (self.version)

    def add_resource_spec(self, resource_spec):
        self.specs[resource_spec.name] = resource_spec
        resource_spec.schema = self

    def add_root(self, name, spec):
        self.specs['root'].add_field(name, spec)


class ResourceSpec(object):
    def __init__(self, name):
        self.name = name
        self.fields = {}
        self.schema = None

    def __repr__(self):
        return "<ResourceSpec %s>" % (self.name)

    def _db(self, collection):
        return self.schema.db[collection]

    def _collection(self):
        return self._db('resource_%s' % (self.name,))

    def add_field(self, name, spec):
        self.fields[name] = spec
        spec.schema = self.schema

    def create_resource(self, field_name, data):
        return Resource(field_name, self, data)

    def default_value(self):
        return None


class FieldSpec(object):
    def __init__(self, field_type):
        self.field_type = field_type
        self.schema = None

    def __repr__(self):
        return "<FieldSpec %s>" % (self.field_type)

    def create_resource(self, field_name, data):
        return Field(field_name, self, data)

    def default_value(self):
        return None


class CollectionSpec(object):
    def __init__(self, target_spec):
        self.target_spec = target_spec
        self.schema = None

    def __repr__(self):
        return "<CollectionSpec %s>" % (self.target_spec)

    @property
    def resource_type(self):
        return self.schema.specs[self.target_spec]

    def create_resource(self, field_name, name):
        return CollectionResource(field_name, self, name)

    def default_value(self):
        return []


class Resource(object):
    def __init__(self, field_name, spec, data):
        self.field_name = field_name
        self.spec = spec
        self.data = data
        self._parent = None

    def __repr__(self):
        return "<Resource %s>" % (self.spec)

    @property
    def _id(self):
        return self.data.get('_id')

    def serialize(self, path):
        fields = {}
        for field_name, field_spec in self.spec.fields.items():
            child = field_spec.create_resource(field_name, self.data.get(field_name))
            if isinstance(child, CollectionResource):
                fields[field_name] = os.path.join(path, field_name)
            else:
                fields[field_name] = child.serialize(os.path.join(path, field_name))
        return fields

    def create_child(self, field_name):
        field_spec = self.spec.fields[field_name]
        field_data = self.data.get(field_name)
        return field_spec.create_resource(field_name, field_name)


class CollectionResource(Resource):
    def __repr__(self):
        return "<CollectionResource %s: %s>" % (self.data, self.spec)

    def serialize(self, path):
        if self._parent:
            resources = self.spec.resource_type._collection().find({
                '_owners': {
                    '$elemMatch': {
                        'owner_spec': self._parent.spec.name,
                        'owner_id': self.parent._id,
                        'owner_field': self.field_name,
                    }
                }
            })
        else:
            resources = self.spec.resource_type._collection().find()
        return [self.spec.resource_type.create_resource(self.field_name, data).serialize(path) for data in resources]

    def create_child(self, child_id):
        data = self.spec.resource_type._collection().find_one(
            {'_id': ObjectId(child_id)})
        resource = self.spec.resource_type.create_resource(self.field_name, data)
        return resource

    def create(self, new_data):
        data = {}
        for field_name, field_spec in self.spec.resource_type.fields.items():
            if field_name in new_data:
                data[field_name] = new_data[field_name]
            else:
                data[field_name] = field_spec.default_value()
        if self._parent:
            data['_owners'] = [{
                'owner_spec': self._parent.spec.name,
                'owner_id': self._parent._id,
                'owner_field': self.field_name
            }]
        return self.spec.resource_type._collection().insert(data)


class Field(Resource):
    def __repr__(self):
        return "<Field %s>" % (self.data,)

    def create_child(self, name):
        raise NotImplementedError(
            '%s is not a traversable resource, its a %s' % (
                self.name, self.spec))

    def serialize(self, path):
        return self.data


class RootResource(Resource):
    def __init__(self, api):
        super(RootResource, self).__init__('root', api.schema.specs["root"], {})

    def __repr__(self):
        return "<RootResource>"

    def create_child(self, path):
        parts = path.split('/')
        root_name = parts.pop(0)

        spec = self.spec.fields[root_name]
        resources = spec.resource_type._collection().find({}, {'_id': 1})
        resource = spec.create_resource(root_name, [str(r_id['_id']) for r_id in resources])
        while parts:
            parent = resource
            resource = resource.create_child(parts.pop(0))
            resource._parent = parent
        return resource

    def serialize(self, path):
        fields = {}
        for field_name, field_spec in self.spec.fields.items():
            fields[field_name] = os.path.join(path, field_name)
        return fields


class MongoApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db
        self.root = RootResource(self)

    def create(self, path, data):
        resource = self.root.create_child(path)
        return resource.create(data)

    def get(self, path):
        path = path.strip('/')
        if path:
            resource = self.root.create_child(path)
            return resource.serialize(os.path.join(self.root_url, path))
        else:
            return self.root.serialize(self.root_url)
