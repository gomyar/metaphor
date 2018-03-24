
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
            if type(spec) in (ResourceLinkSpec, ReverseLinkSpec):
                spec = self.schema.specs[spec.name]
                spec = spec.fields[path.pop(0)]
            elif type(spec) == ResourceSpec:
                spec = spec.fields[path.pop(0)]
            elif type(spec) in (CollectionSpec, LinkCollectionSpec):
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
        self._add_field(name, spec)
        self._link_field(name, spec)

    def _add_field(self, name, spec):
        self.fields[name] = spec
        spec.field_name = name
        spec.parent = self
        spec.schema = self.schema
        if type(spec) == CalcSpec:
            spec.schema.add_calc(spec)

    def _link_field(self, name, spec):
        if type(spec) in (CollectionSpec, ResourceLinkSpec):
            spec.target_spec.add_field("link_%s_%s" % (self.name, name),
                                       ReverseLinkSpec(self.name, name))

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
    def field_type(self):
        return 'link'

    @property
    def target_spec(self):
        return self.schema.specs[self.name]

    @property
    def target_spec_name(self):
        return self.target_spec.name

    def build_resource(self, parent, field_name, data):
        target_resource_spec = self.schema.specs[self.name]
        if not data:
            return NullLinkResource(parent, field_name, target_resource_spec, {})
        resource_data = target_resource_spec._collection().find_one({'_id': ObjectId(data)})
        return target_resource_spec.build_resource(parent, field_name, resource_data)

    def default_value(self):
        return None

    def check_type(self, value):
        return value is None or type(value) is dict


class CalcSpec(Spec):
    def __init__(self, calc_str):
        self.calc_str = calc_str
        self.field_type = 'calc'

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

    def check_type(self, value):
        return False



class ReverseLinkSpec(ResourceLinkSpec):
    def __init__(self, target_spec_name, target_field_name):
        self.name = target_spec_name
        self.target_field_name = target_field_name

    def __repr__(self):
        return "<ReverseLinkSpec %s.%s>" % (self.name, self.target_field_name)

    def serialize(self):
        return {
            'spec': 'reverse_link',
            'name': '%s.%s' % (self.name, self.target_field_name),
        }

    @property
    def field_name(self):
        return self.name

    @field_name.setter
    def field_name(self, value):
        pass

    def _collection(self):
        return self.target_spec._collection()

    def _collection_name(self):
        return self.target_spec._collection_name()

    @property
    def field_type(self):
        return 'link'

    @property
    def target_spec(self):
        return self.schema.specs[self.name]

    def build_resource(self, parent, field_name, data):
        target_resource_spec = self.schema.specs[self.name]
        if isinstance(parent.data.get('_owners', []), list):
            links = [l for l in parent.data.get('_owners', []) if (l['owner_spec'] == self.name and l['owner_field'] == self.target_field_name)]
            if links:
                link = links[0]
                resource_data = target_resource_spec._collection().find_one({'_id': link['owner_id']})
                return target_resource_spec.build_resource(parent, field_name, resource_data)

        return NullLinkResource(parent, field_name, target_resource_spec, {})

    def default_value(self):
        return None

    def check_type(self, value):
        return value is None or type(value) is dict


class FieldSpec(Spec):
    def __init__(self, field_type):
        self.field_type = field_type
        self.field_name = None
        self.schema = None
        self.nullable = True
        self._allowed_types = {
            'str': [str, unicode],
            'int': [int],
            'float': [float, int],
            'bool': [bool],
        }
        self._parse_string = {
            'str': lambda s: s,
            'int': lambda s: int(s),
            'float': lambda s: float(s),
            'bool': lambda s: s[:1].lower() in ('t', '1'),
        }
        self.parent = None

    def __repr__(self):
        return "<FieldSpec %s.%s <%s>>" % (self.parent, self.field_name, self.field_type)

    def __eq__(self, rhs):
        return type(rhs) is FieldSpec and rhs.field_type == self.field_type and rhs.parent == self.parent and rhs.field_name == self.field_name

    def serialize(self):
        return {'spec': 'field', 'type': self.field_type}

    def build_resource(self, parent, field_name, data):
        return Field(parent, field_name, self, data)

    def default_value(self):
        return None

    def check_type(self, value):
        return self.nullable and value is None or type(value) in self._allowed_types.get(self.field_type, [])

    def from_string(self, str_val):
        return self._parse_string[self.field_type](str_val)

class CollectionSpec(Spec):
    def __init__(self, target_spec_name):
        self.target_spec_name = target_spec_name
        self.schema = None

    def serialize(self):
        return {'spec': 'collection', 'target_spec': self.target_spec_name}

    @property
    def field_type(self):
        return 'collection'

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

    def check_type(self, value):
        return False


class LinkCollectionSpec(CollectionSpec):
    @property
    def field_type(self):
        return 'linkcollection'

    def build_resource(self, parent, field_name, name):
        return LinkCollectionResource(parent, field_name, self, name)


# resources


class Resource(object):
    def __init__(self, parent, field_name, spec, data):
        self.field_name = field_name
        self.spec = spec
        self.data = data or {}
        self._parent = parent

    def __repr__(self):
        return "<%s %s \"%s\" at %s>" % (type(self).__name__, self.spec, self.field_name, self.path)

    @property
    def path(self):
        if self._parent:
            return self._parent.path + '/' + (str(self._id) if self._id else self.field_name)
        else:
            return self.field_name

    def _am_link(self):
        if self._parent:
            parent_spec = self._parent.spec
            if type(parent_spec) in (CollectionSpec, LinkCollectionSpec):
                parent_spec = parent_spec.target_spec
            return (type(parent_spec) == ReverseLinkSpec or
                    type(parent_spec.fields[self.field_name]) == ResourceLinkSpec)
        return False

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

    def serialize(self, path, query=None):
        fields = {'id': str(self._id)}
        for field_name, field_spec in self.spec.fields.items():
            child = field_spec.build_resource(self, field_name, self.data.get(field_name))
            if type(field_spec) in (CollectionSpec, LinkCollectionSpec):
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
        fields['self'] = path
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

    def _validate_fields(self, data):
        for name, value in data.items():
            if name not in self.spec.fields:
                raise TypeError('%s not a field of %s' % (name, self.spec.name))
            if not self.spec.fields[name].check_type(value):
                raise TypeError('field type %s cannot be set on %s' % (type(value), self.spec.fields[name]))

    def update(self, data):
        self._validate_fields(data)
        self.data.update(data)
        update = data.copy()
        update['_updated'] = self._update_dict(data.keys())
        self.spec._collection().update({'_id': self._id}, {'$set': update})
        for key in data:
            self.spec.schema.kickoff_update(self, key)

    def _update_dict(self, field_names):
        return {'at': datetime.now(), 'fields': field_names}

    def _add_field_defaults(self, new_data, spec):
        for field_name, field_spec in spec.fields.items():
            if isinstance(field_spec, FieldSpec):
                if field_name not in new_data:
                    new_data[field_name] = field_spec.default_value()

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

    def _create_new(self, parent_field_name, data, spec, owner=None):
        self._add_field_defaults(data, spec)

        resource = spec.build_resource(self, parent_field_name, data)
        resource._validate_fields(data)

        # this is differnt for collections
        if self._parent:
            data['_owners'] = [self._create_owner_link()]

        if owner:
            data['_owners'] = [owner]

        data['_updated'] = self._update_dict(data.keys())

        new_id = spec._collection().insert(data)
        resource.data['_id'] = new_id

        # possibly remove this as a thing
        self._create_new_embedded(resource, new_id, data, spec)

        self.spec.schema.kickoff_create_update(resource)

        return new_id

    def unlink(self):
        if type(self._parent) == LinkCollectionResource:
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
        elif type(self._parent) == CollectionResource:
            updater_ids = self.spec.schema.create_updaters(self)

            self.spec._collection().delete_one({"_id": ObjectId(self._id)})

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


class NullLinkResource(Resource):
    def serialize(self, path, query=None):
        return None

    def create(self, new_data):
        if 'id' in new_data:
            self._update_parent_link(new_data['id'])
            self._parent.spec.schema.kickoff_create_update(self._parent.build_child(self.field_name))
            return new_data['id']
        else:
            new_id = self._create_new(self.field_name, new_data, self.spec)
            self._update_parent_link(str(new_id))
            # may be a problem with links here (also happens twice as _create_new also calls update)
            self._parent.spec.schema.kickoff_create_update(self._parent.build_child(self.field_name))
            return new_id

    def _update_parent_link(self, new_id):
        self._parent.data[self.field_name] = new_id
        self._parent.spec._collection().update({
            "_id": ObjectId(self._parent._id)
        },
        {
            "$set": {self.field_name: ObjectId(new_id)}
        })

    def _create_owner_link(self):
        return {
            'owner_spec': self._parent.spec.name,
            'owner_id': self._parent._id,
            'owner_field': self.field_name
        }

    @property
    def _id(self):
        return self._parent.data[self.field_name] if self._parent else None

    def _create_link(self, resource_id):
        self.spec._collection().update({
            "_id": ObjectId(resource_id)
        },
        {"$push":
            {"_owners": self._create_owner_link()}
        })
        resource = self._load_child_resource(resource_id)
        self.spec.schema.kickoff_create_update(resource)
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

            aggregate_chain = []

            if self._am_link():
                aggregate_chain.append(
                    {'$lookup': {
                        'as': new_owner_prefix,
                        'foreignField': self.field_name,
                        'from': self._parent.collection,
                        'localField': '_id'}})
            else:
                aggregate_chain.append({"$unwind": "$%s_owners" % (owner_prefix,)})
                if type(self.spec) == ReverseLinkSpec:
                    aggregate_chain.append(
                        {"$match": {"%s_owners.owner_spec" % (owner_prefix,): self._parent.spec.name,
                                    "%s_owners.owner_field" % (owner_prefix,): self.spec.target_field_name}})
                else:
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

    def serialize(self, path, query=None):
        aggregate_chain = self.build_aggregate_chain()
        resources = self.spec._collection().aggregate(aggregate_chain)
        serialized = []
        for data in resources:
            resource = self.spec.target_spec.build_resource(self, self.field_name, data)
            serialized.append(resource.serialize(os.path.join(path, str(resource._id))))
        return serialized

    def build_child(self, child_id):
        if child_id in self.spec.target_spec.fields:
            if type(self.spec.target_spec.fields[child_id]) in (CollectionSpec, LinkCollectionSpec):
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

    def serialize(self, path, query=None):
        aggregate_chain = self.build_aggregate_chain()
        resources = self._parent.spec._collection().aggregate(aggregate_chain)
        serialized = []
        for data in resources:
            serialized.append(data)
        return serialized


class CollectionResource(Aggregable, Resource):
    def __repr__(self):
        return "<CollectionResource %s: %s>" % (self.data, self.spec)

    def compile_filter_query(self, filter_str):
        query = dict()
        if filter_str:
            filter_items = [f.split('=', 1) for f in filter_str.split(',')]
            filter_query = dict(filter_items)
            for key, value in filter_query.items():
                query[key] = self.spec.target_spec.fields[key].from_string(value)
        return query

    def compile_ordering(self, ordering_str):
        sort_query = []
        ordering = ordering_str.split(',')
        for order in ordering:
            if order.startswith('-'):
                sort_query.append((order.lstrip('-'), -1))
            else:
                sort_query.append((order, 1))
        return sort_query

    def serialize(self, path, params=None):
        params = params or {}
        query = self.compile_filter_query(params.get('filter', ''))
        if self._parent:
            query.update({
                '_owners': {
                    '$elemMatch': {
                        'owner_spec': self._parent.spec.name,
                        'owner_id': self._parent._id,
                        'owner_field': self.field_name,
                    }
                }
            })
        resources = self.spec._collection().find(query)
        if 'ordering' in params:
            sort_query = self.compile_ordering(params['ordering'])
            resources = resources.sort(sort_query)
        serialized = []
        for data in resources:
            resource = self.spec.target_spec.build_resource(self, self.field_name, data)
            serialized.append(resource.serialize(os.path.join(path, str(resource._id))))
        return serialized

    def build_child(self, child_id):
        if child_id in self.spec.target_spec.fields:
            if type(self.spec.target_spec.fields[child_id]) in (CollectionSpec, LinkCollectionSpec):
                return AggregateResource(self, child_id, self.spec.target_spec.fields[child_id])
            elif type(self.spec.target_spec.fields[child_id]) == ResourceLinkSpec:
                return AggregateResource(self, child_id, self.spec.target_spec.fields[child_id])
            elif type(self.spec.target_spec.fields[child_id]) == ReverseLinkSpec:
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
        return self._create_new(self.field_name, new_data, self.spec.target_spec)

    def _create_owner_link(self):
        return {
            'owner_spec': self._parent.spec.name,
            'owner_id': self._parent._id,
            'owner_field': self.field_name
        }


class LinkCollectionResource(CollectionResource):
    def create(self, new_data):
        return self._create_link(new_data['id'])

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
        self.spec.schema.kickoff_create_update(resource)
        return resource_id


class Field(Resource):
    def __init__(self, parent, field_name, spec, data):
        self.field_name = field_name
        self.spec = spec
        self.data = data
        self._parent = parent

    def __repr__(self):
        return "<Field %s>" % (self.data,)

    def build_child(self, name):
        raise NotImplementedError(
            '%s is not a traversable resource, its a %s' % (
                self.name, self.spec))

    def serialize(self, path, query=None):
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

    def serialize(self, path, query=None):
        fields = {}
        for field_name, field_spec in self.spec.fields.items():
            fields[field_name] = os.path.join(path, field_name)
        return fields

    def build_aggregate_chain(self, chain_path=None):
        return []
