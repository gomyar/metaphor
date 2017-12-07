
import os

from bson.objectid import ObjectId

from metaphor.calclang import parser


class ResourceSpec(object):
    def __init__(self, name):
        self.name = name
        self.fields = {}
        self.schema = None

    def __repr__(self):
        return "<ResourceSpec %s>" % (self.name)

    def serialize(self):
        return {
            'spec': 'resource',
            'name': self.name,
            'fields': dict(
                (name, field.serialize()) for (name, field) in
                self.fields.items())}

    def dependencies(self):
        deps = set()
        for field_name, field_spec in self.fields.items():
            deps.update(field_spec.dependencies())
        return deps

    def _db(self, collection):
        return self.schema.db[collection]

    def _collection(self):
        return self._db('resource_%s' % (self.name,))

    def add_field(self, name, spec):
        self.fields[name] = spec
        spec.schema = self.schema

    def build_resource(self, field_name, data):
        return Resource(field_name, self, data)

    def default_value(self):
        return None


class ResourceLinkSpec(object):
    def __init__(self, name):
        self.name = name

    def serialize(self):
        return {
            'spec': 'resource_link',
            'name': self.name,
        }

    def build_resource(self, field_name, data):
        if not data:
            return NullResource()
        target_resource_spec = self.schema.specs[self.name]
        resource_data = target_resource_spec._collection().find_one({'_id': data})
        return target_resource_spec.build_resource(field_name, resource_data)

    def default_value(self):
        return None

    def dependencies(self):
        return set([self.name])


class CalcSpec(object):
    def __init__(self, calc_str):
        self.calc_str = calc_str

    def build_resource(self, field_name, data):
        return CalcResource(field_name, self, data)

    def parse_calc(self):
        return parser.parse(self.schema, self.calc_str)

    def dependencies(self):
        return self.parse_calc().dependencies()


class FieldSpec(object):
    def __init__(self, field_type):
        self.field_type = field_type
        self.schema = None

    def __repr__(self):
        return "<FieldSpec %s>" % (self.field_type)

    def serialize(self):
        return {'spec': 'field', 'type': self.field_type}

    def build_resource(self, field_name, data):
        return Field(field_name, self, data)

    def default_value(self):
        return None

    def dependencies(self):
        return set()


class CollectionSpec(object):
    def __init__(self, target_spec_name):
        self.target_spec_name = target_spec_name
        self.schema = None

    def serialize(self):
        return {'spec': 'collection', 'target_spec': self.target_spec_name}

    def _collection(self):
        return self.target_spec._collection()

    def __repr__(self):
        return "<CollectionSpec %s>" % (self.target_spec_name)

    @property
    def target_spec(self):
        return self.schema.specs[self.target_spec_name]

    def build_resource(self, field_name, name):
        return CollectionResource(field_name, self, name)

    def default_value(self):
        return []

    def dependencies(self):
        return set([self.target_spec_name])


# resources


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
        fields = {'id': str(self._id)}
        for field_name, field_spec in self.spec.fields.items():
            child = field_spec.build_resource(field_name, self.data.get(field_name))
            if type(field_spec) == CollectionSpec:
                fields[field_name] = os.path.join(path,
                                                  field_name)
            elif type(field_spec) == ResourceLinkSpec:
                if self.data.get(field_name):
                    fields[field_name] = os.path.join(path,
                                                    field_name)
                else:
                    fields[field_name] = None
            else:
                fields[field_name] = child.serialize(os.path.join(path, field_name))
        return fields

    @property
    def root(self):
        root = self
        while root._parent:
            root = root._parent
        return root

    def build_child(self, field_name):
        field_spec = self.spec.fields[field_name]
        field_data = self.data.get(field_name)
        resource = field_spec.build_resource(field_name, field_data)
        resource._parent = self
        return resource

    def update(self, data):
        return self.spec._collection().update({'_id': self._id}, data)

    def _create_new_fields(self, new_data, spec):
        data = {}
        for field_name, field_spec in spec.fields.items():
            if isinstance(field_spec, FieldSpec):
                if field_name in new_data:
                    data[field_name] = new_data[field_name]
                else:
                    data[field_name] = field_spec.default_value()
        return data

    def _recalc_resource(self, resource, spec):
        data = {}
        for field_name, field_spec in spec.fields.items():
            if isinstance(field_spec, CalcSpec):
                calc_field = resource.build_child(field_name)
                data[field_name] = calc_field.calculate()
        return data

    def _create_new_embedded(self, resource, new_id, new_data, spec):
        for field_name, field_spec in spec.fields.items():
            if isinstance(field_spec, ResourceLinkSpec):
                if new_data.get(field_name):
                    embedded_id = resource._create_new(field_name, new_data[field_name], field_spec.schema.specs[field_spec.name])
                    spec._collection().update({'_id': new_id}, {"$set": {field_name: embedded_id}})

    def _create_owner_link(self):
        return {
            'owner_spec': self.spec.name,
            'owner_id': self._id,
            'owner_field': self.field_name
        }

    def _create_new(self, parent_field_name, new_data, spec):
        data = self._create_new_fields(new_data, spec)

        resource = spec.build_resource(parent_field_name, data)

        # remove this
#        data.update(self._recalc_resource(resource, spec))

        # this is differnt for collections
        if self._parent:
            data['_owners'] = [self._create_owner_link()]

        # mark as changed on insert
        data['_changed'] = True # node id ?
        new_id = spec._collection().insert(data)
        resource.data['_id'] = new_id

        # possibly remove this as a thing
        self._create_new_embedded(resource, new_id, new_data, spec)

#        self.schema.kickoff_update(resource)

        data.update(self._recalc_resource(resource, spec))

        # kickoff update worker, join, wait for worker to finish
        # update resource
        #   update resources dependent on this resource

        return new_id


    def unlink(self):
        if self._parent:
            self.spec._collection().update({
                "_id": ObjectId(self._id)
            },
            {"$pull":
                {"_owners":
                    {
                        'owner_spec': self._parent._parent.spec.name,
                        'owner_id': self._parent._parent._id,
                        'owner_field': self._parent.field_name
                    }
                }
            })
            return self._id


class NullResource(Resource):
    def __init__(self):
        pass

    def __repr__(self):
        return "<NullResource>"

    def serialize(self, path):
        return None



class AggregateResource(Resource):
    def __init__(self, field_name, spec, parent_spec):
        self.field_name = field_name
        self.parent_spec = parent_spec
        self.spec = spec

    def __repr__(self):
        return "<AggregateResource %s>" % (self.field_name,)

    def build_aggregate_chain(self, link_name):
        aggregate_chain = []
        aggregate_chain.append(
            {"$lookup": {
                "from": "resource_%s" % (self.parent_spec.name,),
                "localField": "%s_owners.owner_id" % ("_owners_%s." % (link_name,) if link_name else "" ),
                "foreignField": "_id",
                "as": "_owners_%s" % (self.parent_spec.name,),
            }}
        )
        if type(self._parent) == AggregateResource:
            aggregate_chain.extend(self._parent.build_aggregate_chain(self.parent_spec.name))
        elif type(self._parent) == Resource:
            aggregate_chain.append(
                {"$match": {
                    "_owners_%s._id" % (self.parent_spec.name,): self._parent._id
                }},
            )
        elif type(self._parent) == CollectionResource:
            if self._parent._parent:
                aggregate_chain.append(
                    {"$lookup": {
                        "from": "resource_%s" % (self._parent._parent.spec.name,),
                        "localField": "_owners_%s._owners.owner_id" % (self._parent.spec.target_spec_name),
                        "foreignField": "_id",
                        "as": "_owners_%s" % (self._parent._parent.spec.name,),
                    }}
                )
                aggregate_chain.append(
                    {"$match": {
                        "_owners_%s._id" % (self._parent._parent.spec.name,): self._parent._parent._id
                    }},
                )
            else:
                aggregate_chain.append(
                    {"$match": {
                        "_owners_%s" % (self.parent_spec.name,): {"$ne": []}
                    }}
                )
        else:  # root collection
            aggregate_chain.append(
                {"$match": {
                    "_owners_%s" % (self.parent_spec.name,): {"$ne": []}
                }}
            )
        return aggregate_chain


    def serialize(self, path):
        aggregate_chain = self.build_aggregate_chain("")
        resources = self.spec._collection().aggregate(aggregate_chain)
        serialized = []
        for data in resources:
            resource = self.spec.target_spec.build_resource(self.field_name, data)
            serialized.append(resource.serialize(os.path.join(path, str(resource._id))))
        return serialized

    def build_child(self, child_id):
        if child_id in self.spec.target_spec.fields:
            if type(self.spec.target_spec.fields[child_id]) == CollectionSpec:
                aggregate = AggregateResource(child_id, self.spec.target_spec.fields[child_id], self.spec.target_spec)
                aggregate._parent = self
                return aggregate
            if type(self.spec.target_spec.fields[child_id]) == FieldSpec:
                aggregate = AggregateField(child_id, self.spec.target_spec.fields[child_id], self.spec.target_spec)
                aggregate._parent = self
                return aggregate


class AggregateField(AggregateResource):
    def __init__(self, field_name, spec, parent_spec):
        super(AggregateField, self).__init__(field_name, spec, parent_spec)

    def __repr__(self):
        return "<AggregateField: %s>" % (self.field_name,)

    def build_aggregate_chain(self, link_name):
        aggregate_chain = self._parent.build_aggregate_chain(link_name)
        aggregate_chain.append(
            {"$project": {self.field_name: 1}}
        )
        return aggregate_chain

    def serialize(self, path):
        aggregate_chain = self.build_aggregate_chain("")
        resources = self._parent.spec._collection().aggregate(aggregate_chain)
        serialized = []
        for data in resources:
            serialized.append(data)
        return serialized


class CollectionResource(Resource):
    def __repr__(self):
        return "<CollectionResource %s: %s>" % (self.data, self.spec)

    def serialize(self, path):
        if self._parent:
            resources = self.spec._collection().find({
                '_owners': {
                    '$elemMatch': {
                        'owner_spec': self._parent.spec.name,
                        'owner_id': self._parent._id,
                        'owner_field': self.field_name,
                    }
                }
            })
        else:
            resources = self.spec._collection().find()
        serialized = []
        for data in resources:
            resource = self.spec.target_spec.build_resource(self.field_name, data)
            serialized.append(resource.serialize(os.path.join(path, str(resource._id))))
        return serialized

    def build_child(self, child_id):
        if child_id in self.spec.target_spec.fields:
            if type(self.spec.target_spec.fields[child_id]) == CollectionSpec:
                aggregate = AggregateResource(child_id, self.spec.target_spec.fields[child_id], self.spec.target_spec)
                aggregate._parent = self
                return aggregate
                # do nother aggregate
            elif type(self.spec.target_spec.fields[child_id]) == ResourceSpec:
                return AggregateResource()
            elif type(self.spec.target_spec.fields[child_id]) == FieldSpec:
                aggregate = AggregateField(child_id, self.spec.target_spec.fields[child_id], self.spec.target_spec)
                aggregate._parent = self
                return aggregate
            else:
                raise Exception("Cannot aggregate %s" % (
                    child_id))
        else:
            data = self.spec._collection().find_one(
                {'_id': ObjectId(child_id)})
            resource = self.spec.target_spec.build_resource(
                self.field_name, data)
            resource._parent = self
            return resource

    def create(self, new_data):
        if 'id' in new_data:
            return self._create_link(new_data['id'])
        else:
            return self._create_new(self.field_name, new_data, self.spec.target_spec)

    def _create_owner_link(self):
        return {
            'owner_spec': self._parent.spec.name,
            'owner_id': self._parent._id,
            'owner_field': self.field_name
        }

    def _create_link(self, resource_id):
        self.spec._collection().update({
            "_id": ObjectId(resource_id)
        },
        {"$push":
            {"_owners":
                {
                    'owner_spec': self._parent.spec.name,
                    'owner_id': self._parent._id,
                    'owner_field': self.field_name
                }
            }
        })
        return resource_id

    def build_aggregate_chain(self, link_name):
        aggregate_chain = []
        aggregate_chain.append(
            {"$lookup": {
                "from": "resource_%s" % (self._parent.spec.name,),
                "localField": "_owners.owner_id",
                "foreignField": "_id",
                "as": "_owners_%s" % (self._parent.spec.name,),
            }}
        )
        if type(self._parent) == Resource:
            aggregate_chain.append(
                {"$match": {
                    "_owners_%s._id" % (self._parent.spec.name,): self._parent._id
                }},
            )
        else:  # root collection
            aggregate_chain.append(
                {"$match": {
                    "_owners_%s" % (self._parent.spec.name,): {"$ne": []}
                }}
            )
        return aggregate_chain


class Field(Resource):
    def __repr__(self):
        return "<Field %s>" % (self.data,)

    def build_child(self, name):
        raise NotImplementedError(
            '%s is not a traversable resource, its a %s' % (
                self.name, self.spec))

    def serialize(self, path):
        return self.data


class CalcResource(Resource):
    def __repr__(self):
        return "<CalcResource %s - (%s)>" % (
            self.field_name, self.spec.calc_str,)

    def build_child(self, name):
        raise NotImplementedError(
            '%s is not a traversable resource, its a %s' % (
                self, self.spec))

    def serialize(self, path):
        return self.data

    def calculate(self):
        calc = self.spec.parse_calc()
        return calc.calculate(self._parent)


class RootResource(Resource):
    def __init__(self, api):
        super(RootResource, self).__init__('root', api.schema.specs["root"], {})

    def __repr__(self):
        return "<RootResource>"

    def build_child(self, path):
        parts = path.split('/')
        root_name = parts.pop(0)

        spec = self.spec.fields[root_name]
        resource = spec.build_resource(root_name, None)
        while parts:
            resource = resource.build_child(parts.pop(0))
        return resource

    def serialize(self, path):
        fields = {}
        for field_name, field_spec in self.spec.fields.items():
            fields[field_name] = os.path.join(path, field_name)
        return fields


