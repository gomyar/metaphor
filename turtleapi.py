
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

    def add_root(self, path, spec):
        self.specs['root'].add_field(path, spec)


class ResourceSpec(object):
    def __init__(self, name):
        self.name = name
        self.fields = {}
        self.schema = None

    def __repr__(self):
        return "<ResourceSpec %s>" % (self.name)

    def add_field(self, name, spec):
        self.fields[name] = spec
        spec.schema = self.schema

    def create_resource(self, data):
        return Resource(self, data)


class FieldSpec(object):
    def __init__(self, field_type):
        self.field_type = field_type
        self.schema = None

    def __repr__(self):
        return "<FieldSpec %s>" % (self.field_type)

    def create_resource(self, data):
        return Resource(self, data)


class CollectionSpec(object):
    def __init__(self, resource_type):
        self.resource_type = resource_type
        self.schema = None

    def __repr__(self):
        return "<CollectionSpec %s>" % (self.resource_type)

    def create_resource(self, name):
        return CollectionResource(self, name)


class Resource(object):
    def __init__(self, spec, data):
        self.spec = spec
        self.data = data

    def __repr__(self):
        return "<Resource %s>" % (self.spec)

    def _db(self, collection):
        return self.spec.schema.db[collection]

    def _collection(self):
        return self._db('resource_%s' % (self.spec.resource_type.name,))

    def serialize(self):
        fields = {}
        for field_name, field_type in self.spec.fields.items():
            fields[field_name] = self.data[field_name]
        return fields

    def create_child(self, field_name):
        field_spec = self.spec.fields[field_name]
        field_data = self.data[field_name]
        return field_spec.create_resource(field_name)


class CollectionResource(Resource):
    def __repr__(self):
        return "<CollectionResource %s: %s>" % (self.data, self.spec)

    def serialize(self):
        data = self._collection().find()
        resources = [self.spec.create_resource(res) for res in data]
        return resources

    def create_child(self, child_id):
        data = self._collection().find_one({'_id': ObjectId(child_id)})
        resource = self.spec.resource_type.create_resource(data)
        return resource


class Field(Resource):
    def __init__(self, name, data):
        self.name = name
        self.data = data

    def __repr__(self):
        return "<Field %s=%s>" % (self.name, self.value)

    def create_child(self, path):
        raise NotImplementedError('%s is not a traverable resource' % (
                                  self.name))

class RootResource(Resource):
    def __init__(self, api):
        super(RootResource, self).__init__(api.schema.specs["root"], {})

    def __repr__(self):
        return "<RootResource>"

    def create_child(self, path):
        path = path.split('/')
        root_name = path.pop(0)

        spec = self.spec.fields[root_name]
        resource = spec.create_resource(root_name)
        while path:
            resource = resource.create_child(path.pop(0))
        return resource


class MongoApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db
        self.root = RootResource(self)

    def _create_resource(self, spec, data=None):
        return spec.create_resource(self, data)

    def _resource_name(self, path):
        '''
            /collection/id/collection
            /collection/id/field
        '''
        return path.strip('/').split('/')[-1]

    def create(self, path, data):
        collection_name = self._resource_name(path)
        return self.db[collection_name].insert(data)

    def _get(self, resource, resource_id):
        return self.db[resource].find_one({'_id': ObjectId(resource_id)})

    def get(self, path):
        resource = self.root.create_child(path)
        return resource.serialize()
