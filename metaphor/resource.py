
import os

from bson.objectid import ObjectId

from metaphor.calclang import parser

from datetime import datetime


class Spec(object):
    def resolve_spec_hier(self, resource_ref):
        path = resource_ref.split('.')
        if path[0] == 'self':
            path.pop(0)
            spec = self.parent
        else:
            spec = self.schema.root_spec
        found = [spec]
        while path:
            if type(spec) == ResourceLinkSpec:
                spec = self.schema.specs[spec.name]
                spec = spec.fields[path.pop(0)]
            elif type(spec) == ResourceSpec:
                spec = spec.fields[path.pop(0)]
            elif type(spec) == CollectionSpec:
                spec = spec.target_spec.fields[path.pop(0)]
            else:
                raise Exception("Cannot resolve spec %s" % (spec,))
            found.append(spec)
        return found

    def resolve_spec(self, resource_ref):
        found = self.resolve_spec_hier(resource_ref)
        return found[-1]


class ResourceSpec(Spec):
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

    def _db(self, collection):
        return self.schema.db[collection]

    def _collection(self):
        return self._db(self._collection_name())

    def _collection_name(self):
        return 'resource_%s' % (self.name,)

    def add_field(self, name, spec):
        self.fields[name] = spec
        spec.field_name = name
        spec.parent = self
        spec.schema = self.schema
        if type(spec) == CalcSpec:
            spec.schema.add_calc(spec)

    def build_resource(self, parent, field_name, data):
        return Resource(parent, field_name, self, data)

    def default_value(self):
        return None


class ResourceLinkSpec(Spec):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<ResourceLinkSpec %s>" % (self.name,)

    def serialize(self):
        return {
            'spec': 'resource_link',
            'name': self.name,
        }

    def _collection(self):
        return self.target_spec._collection()

    def _collection_name(self):
        return self.target_spec._collection_name()

    @property
    def target_spec(self):
        return self.schema.specs[self.name]

    def build_resource(self, parent, field_name, data):
        target_resource_spec = self.schema.specs[self.name]
        if not data:
            return LinkResource(parent, field_name, target_resource_spec, {})
        resource_data = target_resource_spec._collection().find_one({'_id': ObjectId(data)})
        return target_resource_spec.build_resource(parent, field_name, resource_data)

    def default_value(self):
        return None


class CalcSpec(Spec):
    def __init__(self, calc_str):
        self.calc_str = calc_str

    def __repr__(self):
        return "<CalcSpec %s.%s '%s'>" % (self.parent.name, self.field_name, self.calc_str,)

    def build_resource(self, parent, field_name, data):
        return CalcField(parent, field_name, self, data)

    def parse_calc(self):
        return parser.parse(self.schema, self.calc_str)

    def all_resource_refs(self):
        return self.parse_calc().all_resource_refs()

    def serialize(self):
        return {'spec': 'calc', 'calc': self.calc_str}


class FieldSpec(Spec):
    def __init__(self, field_type):
        self.field_type = field_type
        self.field_name = None
        self.schema = None

    def __repr__(self):
        return "<FieldSpec %s.%s <%s>>" % (self.parent.name, self.field_name, self.field_type)

    def __eq__(self, rhs):
        return type(rhs) is FieldSpec and rhs.field_type == self.field_type and rhs.parent.name == self.parent.name and rhs.field_name == self.field_name

    def serialize(self):
        return {'spec': 'field', 'type': self.field_type}

    def build_resource(self, parent, field_name, data):
        return Field(parent, field_name, self, data)

    def default_value(self):
        return None


class CollectionSpec(Spec):
    def __init__(self, target_spec_name):
        self.target_spec_name = target_spec_name
        self.schema = None

    def serialize(self):
        return {'spec': 'collection', 'target_spec': self.target_spec_name}

    @property
    def name(self):
        return self.target_spec_name

    def _collection(self):
        return self.target_spec._collection()

    def _collection_name(self):
        return self.target_spec._collection_name()

    def __repr__(self):
        return "<CollectionSpec %s>" % (self.target_spec_name)

    @property
    def target_spec(self):
        return self.schema.specs[self.target_spec_name]

    def build_resource(self, parent, field_name, name):
        return CollectionResource(parent, field_name, self, name)

    def default_value(self):
        return []


# resources


class Resource(object):
    def __init__(self, parent, field_name, spec, data):
        self.field_name = field_name
        self.spec = spec
        self.data = data
        self._parent = parent

    def __repr__(self):
        return "<%s %s [%s] at %s>" % (type(self).__name__, self.field_name, self.spec, self.path)

    @property
    def path(self):
        if self._parent:
            return self._parent.path + '/' + (str(self._id) if self._id else self.field_name)
        else:
            return self.field_name

    @property
    def collection(self):
        return self.spec._collection_name()

    def build_child_dot(self, path):
        parts = path.split('.')
        root_name = parts.pop(0)

        if root_name == 'self':
            resource = self
        else:
            resource = self.spec.schema.root.build_child(root_name)

        while parts:
            resource = resource.build_child(parts.pop(0))
        return resource

    def build_aggregate_path(self, chain_path=None):
        chain_path = chain_path or ""
        if self._parent:
            return self._parent.build_aggregate_path(chain_path + "__" + self.spec.name)
        else:
            return chain_path + "__" + self.spec.name

    def build_aggregate_chain(self, chain_path=None):
        if chain_path is None:
            raise Exception("Resource cannot be first-order aggregate")

        chain_path = chain_path or ""
        aggregate_chain = []

        owner_prefix = chain_path + '.' if chain_path else ""
        new_owner_prefix = chain_path + "__" + self.spec.name

        aggregate_chain.append(
            {"$match": {
                "%s_id" % (owner_prefix,): self._id
            }}
        )
        if self._parent:
            aggregate_chain.extend(self._parent.build_aggregate_chain(chain_path))
        return aggregate_chain

    def create_resource_ref(self):
        if self._parent:
            parent_ref = self._parent.create_resource_ref()
            if parent_ref:
                return parent_ref + '.' + self.field_name
            else:
                return self.field_name
        elif self._id:
            return ''
        else:
            return 'self'

    @property
    def _id(self):
        return self.data.get('_id') if self.data else None

    def serialize(self, path):
        fields = {'id': str(self._id)}
        for field_name, field_spec in self.spec.fields.items():
            child = field_spec.build_resource(self, field_name, self.data.get(field_name))
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
        return field_spec.build_resource(self, field_name, field_data)

    def update(self, data):
        self.data.update(data)
        update = data.copy()
        update['_updated'] = self._update_dict(data.keys())
        self.spec._collection().update({'_id': self._id}, {'$set': update})
        for key in data:
            self.spec.schema.kickoff_update(self, key)

    def _update_dict(self, field_names):
        return {'at': datetime.now(), 'fields': field_names}

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
                    embedded_id = resource._create_new(field_name, new_data[field_name], field_spec.schema.specs[field_spec.name],
                                                       {'owner_spec': spec.name,
                                                        'owner_id': new_id,
                                                        'owner_field': field_name,
                                                       })
                    spec._collection().update({'_id': new_id}, {"$set": {field_name: embedded_id,
                                                                         '_updated': self._update_dict([field_name])}})

    def _create_owner_link(self):
        return {
            'owner_spec': self.spec.name,
            'owner_id': self._id,
            'owner_field': self.field_name
        }

    def _create_new(self, parent_field_name, new_data, spec, owner=None):
        data = self._create_new_fields(new_data, spec)

        resource = spec.build_resource(self, parent_field_name, data)

        # this is differnt for collections
        if self._parent:
            data['_owners'] = [self._create_owner_link()]

        if owner:
            data['_owners'] = [owner]

        data['_updated'] = self._update_dict(data.keys())

        new_id = spec._collection().insert(data)
        resource.data['_id'] = new_id

        # possibly remove this as a thing
        self._create_new_embedded(resource, new_id, new_data, spec)

        self.spec.schema.kickoff_create_update(self, resource)

        return new_id

    def unlink(self):
        if type(self._parent) == CollectionResource:
            updater_ids = self.spec.schema.create_updaters(self)

            parent_owner = {
                        'owner_spec': self._parent._parent.spec.name,
                        'owner_id': self._parent._parent._id,
                        'owner_field': self._parent.field_name
                    }
            self.spec._collection().update({
                "_id": ObjectId(self._id)
            },
            {"$pull":
                {"_owners": parent_owner}
            })

            self.spec.schema.run_updaters(updater_ids)
        else:
            updater_ids = self.spec.schema.create_updaters(self)

            parent_owner = {
                        'owner_spec': self._parent.spec.name,
                        'owner_id': self._parent._id,
                        'owner_field': self.field_name
                    }
            self.spec._collection().update({
                "_id": ObjectId(self._id)
            },
            {"$pull":
                {"_owners": parent_owner}
            })
            self._parent.data[self.field_name] = None
            self._parent.spec._collection().update(
                {'_id': self._parent._id},
                {'$set': {self.field_name: None}})

            self.spec.schema.run_updaters(updater_ids)

        return self._id



class LinkResource(Resource):
    def serialize(self, path):
        return None

    def create(self, new_data):
        if 'id' in new_data:
            new_link = self._create_link(new_data['id'])
            self._update_parent_link(new_data['id'])
            return new_link
        else:
            new_resource = self._create_new(self.field_name, new_data, self.spec)
            self._update_parent_link(str(new_resource))
            return new_resource

    def _update_parent_link(self, new_id):
        self._parent.spec._collection().update({
            "_id": ObjectId(self._parent._id)
        },
        {
            "$set": {self.field_name: new_id}
        })

    def _create_owner_link(self):
        return {
            'owner_spec': self._parent.spec.name,
            'owner_id': self._parent._id,
            'owner_field': self.field_name
        }

    @property
    def _id(self):
        return None

    def _create_link(self, resource_id):
        self.spec._collection().update({
            "_id": ObjectId(resource_id)
        },
        {"$push":
            {"_owners": self._create_owner_link()}
        })
        resource = self._load_child_resource(resource_id)
        self.spec.schema.kickoff_create_update(self, resource)
        return resource_id

    def _load_child_resource(self, child_id):
        data = self.spec._collection().find_one(
            {'_id': ObjectId(child_id)})
        resource = self.spec.build_resource(
            self, self.field_name, data)
        return resource


class Aggregable(object):
    def build_aggregate_chain(self, chain_path=None):
        chain_path = chain_path or ""
        aggregate_chain = []


        if self._parent:
            owner_prefix = chain_path + '.' if chain_path else ""
            new_owner_prefix = chain_path + "__" + self._parent.spec.name
            aggregate_chain = [{"$unwind": "$%s_owners" % (owner_prefix,)}]
            aggregate_chain.append(
                {"$match": {"%s_owners.owner_spec" % (owner_prefix,): self._parent.spec.name,
                            "%s_owners.owner_field" % (owner_prefix,): self.field_name}})
            aggregate_chain.append(
                {"$lookup": {
                    "from": self._parent.collection,
                    "localField": "%s_owners.owner_id" % (owner_prefix,),
                    "foreignField": "_id",
                    "as": new_owner_prefix,
                }})
            aggregate_chain.append(
                {"$unwind": "$%s" % (new_owner_prefix,)}
            )
            aggregate_chain.extend(self._parent.build_aggregate_chain(new_owner_prefix))
        return aggregate_chain


class AggregateResource(Aggregable, Resource):
    def __init__(self, parent, field_name, spec):
        super(AggregateResource, self).__init__(parent, field_name, spec, None)

    def serialize(self, path):
        aggregate_chain = self.build_aggregate_chain("")
        resources = self.spec._collection().aggregate(aggregate_chain)
        serialized = []
        for data in resources:
            resource = self.spec.target_spec.build_resource(self, self.field_name, data)
            serialized.append(resource.serialize(os.path.join(path, str(resource._id))))
        return serialized

    def build_child(self, child_id):
        if child_id in self.spec.target_spec.fields:
            if type(self.spec.target_spec.fields[child_id]) == CollectionSpec:
                return AggregateResource(self, child_id, self.spec.target_spec.fields[child_id])
            if type(self.spec.target_spec.fields[child_id]) == FieldSpec:
                return AggregateField(self, child_id, self.spec.target_spec.fields[child_id])
            if type(self.spec.target_spec.fields[child_id]) == ResourceLinkSpec:
                return AggregateResource(self, child_id, self.spec.target_spec.fields[child_id])


class AggregateField(AggregateResource):
    def __repr__(self):
        return "<AggregateField: %s>" % (self.field_name,)

    def build_aggregate_chain(self, link_name=None):
        return self._parent.build_aggregate_chain(link_name)

    def build_aggregate_path(self, chain_path=None):
        return self._parent.build_aggregate_path()

    def serialize(self, path):
        aggregate_chain = self.build_aggregate_chain("")
        resources = self._parent.spec._collection().aggregate(aggregate_chain)
        serialized = []
        for data in resources:
            serialized.append(data)
        return serialized


class CollectionResource(Aggregable, Resource):
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
            resource = self.spec.target_spec.build_resource(self, self.field_name, data)
            serialized.append(resource.serialize(os.path.join(path, str(resource._id))))
        return serialized

    def build_child(self, child_id):
        if child_id in self.spec.target_spec.fields:
            if type(self.spec.target_spec.fields[child_id]) == CollectionSpec:
                return AggregateResource(self, child_id, self.spec.target_spec.fields[child_id])
            elif type(self.spec.target_spec.fields[child_id]) == ResourceLinkSpec:
                return AggregateResource(self, child_id, self.spec.target_spec.fields[child_id])
            elif type(self.spec.target_spec.fields[child_id]) == FieldSpec:
                return AggregateField(self, child_id, self.spec.target_spec.fields[child_id])
            elif type(self.spec.target_spec.fields[child_id]) == CalcSpec:
                return AggregateField(self, child_id, self.spec.target_spec.fields[child_id])
            else:
                raise Exception("Cannot aggregate %s" % (
                    child_id))
        else:
            resource = self._load_child_resource(child_id)
            return resource

    def _load_child_resource(self, child_id):
        data = self.spec._collection().find_one(
            {'_id': ObjectId(child_id)})
        resource = self.spec.target_spec.build_resource(
            self, self.field_name, data)
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
        resource = self._load_child_resource(resource_id)
        self.spec.schema.kickoff_create_update(self, resource)
        return resource_id


class Field(Resource):
    def __repr__(self):
        return "<Field %s>" % (self.data,)

    def build_child(self, name):
        raise NotImplementedError(
            '%s is not a traversable resource, its a %s' % (
                self.name, self.spec))

    def serialize(self, path):
        return self.data


class CalcField(Field):
    def __repr__(self):
        return "<CalcField %s - (%s)>" % (
            self.field_name, self.spec.calc_str,)

    def calculate(self):
        calc = self.spec.parse_calc()
        return calc.calculate(self._parent)


class RootResource(Resource):
    def __init__(self, schema):
        super(RootResource, self).__init__(None, 'root', schema.specs["root"], {})

    def __repr__(self):
        return "<RootResource>"

    def build_child(self, path):
        parts = path.split('/')
        root_name = parts.pop(0)

        spec = self.spec.fields[root_name]
        resource = spec.build_resource(None, root_name, None)
        while parts:
            resource = resource.build_child(parts.pop(0))
        return resource

    def serialize(self, path):
        fields = {}
        for field_name, field_spec in self.spec.fields.items():
            fields[field_name] = os.path.join(path, field_name)
        return fields

    def build_aggregate_chain(self, chain_path=None):
        return []
