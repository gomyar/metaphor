
from datetime import timedelta
import tokenize
from io import StringIO
from metaphor.schema import Field


class Operable(object):
    pass


class Calc(Operable):
    def __init__(self, tokens, parser):
        self.tokens = tokens
        self._parser = parser

    def __repr__(self):
        return "C[%s]" % (self.tokens,)

    def calculate(self, self_id):
        raise NotImplemented()

    def is_collection(self):
        return False



class ResourceRef(Operable):
    def __init__(self, resource_ref, field_name, parser, spec):
        self._parser = parser
        self.resource_ref = resource_ref
        self.field_name = field_name
        self.spec = spec

    def root_collection(self):
        return self.resource_ref.root_collection()

    def get_resource_dependencies(self):
        return self.resource_ref.get_resource_dependencies()

    def infer_type(self):
        return self.resource_ref.infer_type().build_child_spec(self.field_name)

    def is_collection(self):
        return self.resource_ref.is_collection()

    def is_primitive(self):
        return self.spec.is_primitive()

    def calculate(self, self_id):
        aggregate_query, spec, is_aggregate = self.aggregation(self_id)
        # run mongo query from from root_resource collection
        cursor = self.root_collection().aggregate(aggregate_query)

        results = [row for row in cursor]
        if is_aggregate:
            return results
        elif spec.is_field():
            return results[0].get(self.field_name) if results else None
        else:
            return results[0]

    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        return aggregation, child_spec, is_aggregate

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        # create own agg
        agg = self.create_reverse(calc_spec_name, calc_field_name)

        # get all subsequent aggs
        aggregations = self.resource_ref.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

        # prepend own agg to initial (ignored) agg chain
        aggregations[0] = agg + aggregations[0]

        # if own spec same as changed resource spec
        if self.spec == resource_spec:
            # duplicate initial agg chain, add match, and return
            aggregations.insert(0, list(aggregations[0]))
#            if resource_id:
#                aggregations[1].insert(0, {"$match": {"_id": self.spec.schema.decodeid(resource_id)}})
        return aggregations

    def __repr__(self):
        return "R[%s %s]" % (self.resource_ref, self.field_name)

    def resource_ref_snippet(self):
        return self.resource_ref.resource_ref_snippet() + '_' + self.field_name

    def validate(self):
        pass

    def _is_lookup(self):
        return self.resource_ref._is_lookup()

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user)


class FieldRef(ResourceRef, Calc):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        aggregation.append(
            {"$addFields": {
                self.field_name: True,
            }})
        return aggregation, child_spec, is_aggregate

    def get_resource_dependencies(self):
        return self.resource_ref.get_resource_dependencies() | {'%s.%s' % (self.spec.spec.name, self.field_name)}

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

    def _create_calc_agg_tree(self):
        return {"_v_%s" % self.resource_ref_snippet(): self}

    def _is_lookup(self):
        return self.resource_ref._is_lookup()

    def _create_calc_expr(self):
        return "$_v_%s" % self.resource_ref_snippet()

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$addFields": {"_val": "$%s" % self.field_name}},
        ]


class RootResourceRef(ResourceRef):
    def __init__(self, resource_name, parser, spec):
        self.resource_name = resource_name
        self.parent_spec = None
        self._parser = parser
        if self.resource_name == 'self':
            self.spec = spec
        elif self.resource_name == 'ego':
            self.spec = spec.schema.specs['user']
        else:
            self.spec = self.root_spec(spec.schema)

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        if self.resource_name in ["self", "ego"]:
            return [[], []]
        else:
            # assuming root / collection
            return [[], self.create_reverse(calc_spec_name, calc_field_name)]

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                "from": "resource_%s" % (calc_spec_name,),
                "as": "_field_%s" % (calc_field_name,),
                "pipeline": [],
            }},
            {'$group': {'_id': '$_field_%s' % (calc_field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def validate(self):
        if self.resource_name != "self" and self.resource_name not in self.spec.schema.root.fields:
            raise SyntaxError("No such resource: %s" % self.resource_name)

    def root_spec(self, schema):
        return schema.specs[
            schema.root.fields[self.resource_name].target_spec_name]

    def root_collection(self):
        return self.spec.schema.db['resource_%s' % self.spec.name]

    def get_resource_dependencies(self):
        if self.resource_name == 'self':
            return set()
        else:
            return {"root.%s" % self.resource_name}

    def infer_type(self):
        return self.spec

    def aggregation(self, self_id, user=None):
        if self.resource_name == 'self':
            aggregation = [
                {"$match": {"_grants": {"$in": user.grants}}}
            ] if user else []
#            aggregation.extend([
#                {"$match": {"_id": self.spec.schema.decodeid(self_id)}}
#            ])
            return aggregation, self.spec, False
        elif self.resource_name == 'ego':
            aggregation = [
            # using ego as dummy 
#                {"$match": {"username": user.username}}
            ]
            return aggregation, self.spec, False
        else:
            aggregation = [
                {"$match": {"_grants": {"$in": user.grants}}}
            ] if user else []
            aggregation.extend([
                {"$match": {"$and": [
                    {"_parent_field_name": self.resource_name},
                    {"_parent_canonical_url": '/'},
                ]}}
            ])
            return aggregation, self.spec, True

    def is_collection(self):
        if self.resource_name == 'self':
            return False
        else:
            return True

    def __repr__(self):
        return "T[%s]" % (self.resource_name,)

    def resource_ref_snippet(self):
        return self.resource_name

    def _is_lookup(self):
        return self.resource_name != 'self'

    def create_aggregation(self, user=None):
        if self.resource_name == 'self':
            return [
#                {"$lookup": {
#                    "from": "resource_%s" % self.spec.name,
#                    "as": "_val",
#                    "let": {"id": "$_id"},
#                    "pipeline":[
#                        {"$match": {"$expr": {
#                            "_id": "$$id",
#                        }}}
#                    ]
#                }},
#                {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},
            ]
        else:
            return [
                {"$lookup": {
                    "from": "resource_%s" % self.spec.name,
                    "as": "_val",
                    "pipeline":[
                        {"$match": {
                            "_parent_field_name": self.resource_name,
                            "_parent_canonical_url": '/',
                            # TODO: _deleted: false
                            "_deleted": {"$exists": False},
                        }}
                    ]
                }},
                {'$group': {'_id': '$_val'}},
                {"$unwind": "$_id"},
                {"$replaceRoot": {"newRoot": "$_id"}},
            ]


class IDResourceRef(ResourceRef):
    def __init__(self, root_resource_ref, resource_id, parser, spec):
        self.resource_ref = root_resource_ref
        self.resource_id = resource_id
        self._parser = parser
        self.spec = spec

    def infer_type(self):
        return self.spec

    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        aggregation.append(
            {"$match": {"_id": self.spec.schema.decodeid(self.resource_id)}}
        )
        return aggregation, spec, False

    def is_collection(self):
        return False

    def __repr__(self):
        return "I[%s.%s]" % (self.resource_ref, self.resource_id,)

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$match": {
                "_id": self.spec.schema.decodeid(self.resource_id),
                "_deleted": {"$exists": False},
            }}
        ]


class CollectionResourceRef(ResourceRef):
    def __init__(self, resource_ref, field_name, parser, spec, parent_spec):
        super(CollectionResourceRef, self).__init__(resource_ref, field_name, parser, spec)
        self.parent_spec = parent_spec

    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        # if linkcollection / collection
        aggregation.append(
            {"$lookup": {
                    "from": "resource_%s" % (child_spec.name,),
                    "localField": "_id",
                    "foreignField": "_parent_id",
                    "as": "_field_%s" % (self.field_name,),
            }})
        aggregation.append(
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
        )
        aggregation.append(
            {"$unwind": "$_id"}
        )
        aggregation.append(
            {"$replaceRoot": {"newRoot": "$_id"}}
        )
        return aggregation, child_spec, True

    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                "from": "resource_%s" % (self.parent_spec.name,),
                "localField": "_parent_id",
                "foreignField": "_id",
                "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$lookup": {
                    "from": "resource_%s" % self.spec.name,
                    "as": "_val",
                    "let": {"s_id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$eq": ["$_parent_id", "$$s_id"],
                            }
                        }},
                        {"$match": {
                            "_deleted": {"$exists": False},
                        }},
                    ]
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]


class LinkCollectionResourceRef(ResourceRef):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        # if linkcollection / collection
        aggregation.append(
            {"$lookup": {
                    "from": "resource_%s" % (child_spec.name,),
                    "localField": "%s._id" % self.field_name,
                    "foreignField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }})
        aggregation.append(
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
        )
        aggregation.append(
            {"$unwind": "$_id"}
        )
        aggregation.append(
            {"$replaceRoot": {"newRoot": "$_id"}}
        )
        if user:
            aggregation.append(
                {"$match": {"_grants": {"$in": user.grants}}}
            )
        return aggregation, child_spec, True

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "resource_%s" % (self.resource_ref.spec.name,),
                    "foreignField": "%s._id" % self.field_name,
                    "localField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$lookup": {
                    "from": "resource_%s" % self.spec.name,
                    "as": "_val",
                    "let": {"s_id": {"$ifNull": ["$%s" % self.field_name, []]}},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$in": [{"_id": "$_id"}, "$$s_id"],
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]


class OrderedCollectionResourceRef(LinkCollectionResourceRef):
    pass


class LinkResourceRef(ResourceRef):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        # if linkcollection / collection
        # if link
        aggregation.append(
            {"$lookup": {
                    "from": "resource_%s" % (child_spec.name,),
                    "localField": self.field_name,
                    "foreignField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }})
        aggregation.append(
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
        )
        aggregation.append(
            {"$unwind": "$_id"}
        )
        aggregation.append(
            {"$replaceRoot": {"newRoot": "$_id"}}
        )
        if user:
            aggregation.append(
                {"$match": {"_grants": {"$in": user.grants}}}
            )
        return aggregation, child_spec, is_aggregate

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "resource_%s" % (self.resource_ref.spec.name,),
                    "localField": "_id",
                    "foreignField": self.field_name,
                    "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$lookup": {
                "from": 'resource_%s' % self.spec.name,
                "as": '_val',
                "let": {"s_id": "$%s" % self.field_name},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$eq": ["$_id", "$$s_id"],
                        }
                    }},
                    {"$match": {
                        "_deleted": {"$exists": False},
                    }},
                ]
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]


class CalcResourceRef(ResourceRef, Calc):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        calc_tree = spec.schema.calc_trees[spec.name, self.field_name]
        calc_spec = calc_tree.infer_type()
        if spec.fields[self.field_name].is_primitive():
            # int float str
            pass
        else:
            # resource
            # lookup
            aggregation.append(
                {"$lookup": {
                        "from": "resource_%s" % (calc_spec.name,),
                        "localField": "%s._id" % self.field_name,
                        "foreignField": "_id",
                        "as": "_field_%s" % (self.field_name,),
                }})
            aggregation.append(
                {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
            )
            aggregation.append(
                {"$unwind": "$_id"}
            )
            aggregation.append(
                {"$replaceRoot": {"newRoot": "$_id"}}
            )
            if user:
                aggregation.append(
                    {"$match": {"_grants": {"$in": user.grants}}}
                )
            is_aggregate = is_aggregate or calc_tree.is_collection()
        return aggregation, calc_spec, is_aggregate

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "resource_%s" % (self.resource_ref.spec.name,),
                    "foreignField": self.field_name,
                    "localField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _create_calc_agg_tree(self):
        return {"_v_%s" % self.field_name: self}

    def _create_calc_expr(self):
        return "$%s" % self.field_name

    def create_aggregation(self, user=None):
        agg = self.resource_ref.create_aggregation(user)
        calc_tree = self.resource_ref.spec.schema.calc_trees[self.resource_ref.spec.name, self.field_name]
        calc_spec = calc_tree.infer_type()
        if calc_tree.is_primitive():
            return agg + [
                {"$addFields": {"_val": "$%s" % self.field_name}},
            ]
        else:
            return agg + [
                {"$lookup": {
                        "from": "resource_%s" % (calc_spec.name,),
                        "localField": "%s._id" % self.field_name,
                        "foreignField": "_id",
                        "as": "_val",
                }},
                {'$group': {'_id': '$_val'}},
                {"$unwind": "$_id"},
                {"$replaceRoot": {"newRoot": "$_id"}},
            ]


class ReverseLinkResourceRef(ResourceRef):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        aggregation.append(
            {"$lookup": {
                    "from": "resource_%s" % (child_spec.name,),
                    "localField": "_id",
                    "foreignField": spec.fields[self.field_name].reverse_link_field,
                    "as": "_field_%s" % (self.field_name,),
            }})
        aggregation.append(
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
        )
        aggregation.append(
            {"$unwind": "$_id"}
        )
        aggregation.append(
            {"$replaceRoot": {"newRoot": "$_id"}}
        )
        if user:
            aggregation.append(
                {"$match": {"_grants": {"$in": user.grants}}}
            )
        return aggregation, child_spec, True

    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        _, reverse_spec, reverse_field = self.field_name.split('_')  # well this should have a better impl
        return {"%s.%s" % (reverse_spec, reverse_field)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "resource_%s" % (self.resource_ref.spec.name,),
                    "localField": self.resource_ref.spec.fields[self.field_name].reverse_link_field,
                    "foreignField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$lookup": {
                    "from": "resource_%s" % (self.spec.name,),
                    "localField": "_id",
                    "foreignField": self.resource_ref.spec.fields[self.field_name].reverse_link_field,
                    "as": "_val",
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
            # TODO: match _deleted: false ??
        ]


class ParentCollectionResourceRef(ResourceRef):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        aggregation.append(
            {"$lookup": {
                    "from": "resource_%s" % (child_spec.name,),
                    "localField": "_parent_id",
                    "foreignField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }})
        aggregation.append(
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
        )
        aggregation.append(
            {"$unwind": "$_id"}
        )
        aggregation.append(
            {"$replaceRoot": {"newRoot": "$_id"}}
        )
        if user:
            aggregation.append(
                {"$match": {"_grants": {"$in": user.grants}}}
            )
        return aggregation, child_spec, False

    def is_collection(self):
        return False

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "resource_%s" % (self.resource_ref.spec.name,),
                    "foreignField": "_parent_id",
                    "localField": "_id",
                    "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$lookup": {
                    "from": "resource_%s" % (self.spec.name,),
                    "localField": "_parent_id",
                    "foreignField": "_id",
                    "as": "_val",
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
            # TODO: match _deleted: false ??
        ]


class ReverseLinkCollectionResourceRef(ResourceRef):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        # when a reverse aggregate is followed by another reverse aggregate
        # reverse link to collection (through _owners)
        aggregation.append(
            {"$lookup": {
                    "from": "resource_%s" % (child_spec.name,),
                    "localField": '_id',
                    "foreignField": spec.fields[self.field_name].reverse_link_field + "._id",
                    "as": "_field_%s" % (self.field_name,),
            }})
        aggregation.append(
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
        )
        aggregation.append(
            {"$unwind": "$_id"}
        )
        aggregation.append(
            {"$replaceRoot": {"newRoot": "$_id"}}
        )
        if user:
            aggregation.append(
                {"$match": {"_grants": {"$in": user.grants}}}
            )
        return aggregation, child_spec, True

    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        _, reverse_spec, reverse_field = self.field_name.split('_')  # well this should have a better impl
        return {"%s.%s" % (reverse_spec, reverse_field)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "resource_%s" % (self.resource_ref.spec.name,),
                    "foreignField": "_id",
                    "localField": "%s._id" % (self.resource_ref.spec.fields[self.field_name].reverse_link_field,),
                    "as": "_field_%s" % (self.field_name,),
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + [
            {"$lookup": {
                    #"from": "resource_%s" % (self.resource_ref.spec.name,),
                    "from": "resource_%s" % (self.resource_ref.spec.fields[self.field_name].target_spec_name,),
                    "localField": '_id',
                    "foreignField": self.resource_ref.spec.fields[self.field_name].reverse_link_field + "._id",
                    "as": "_val",
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
            # TODO: match _deleted: false ??
        ]


class FilteredResourceRef(ResourceRef):
    def __init__(self, root_resource_ref, filter_ref, parser, spec):
        self._parser = parser
        self.resource_ref = root_resource_ref
        self.filter_ref = filter_ref
        self.spec = spec

    def validate(self):
        self.resource_ref.validate()
        self.filter_ref.validate_ref(self.spec)

    def infer_type(self):
        return self.resource_ref.infer_type()

    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        filter_agg = self.filter_ref.filter_aggregation(spec, self_id)
        aggregation.append(filter_agg)
        return aggregation, spec, True

    def is_collection(self):
        return True

    def resource_ref_snippet(self):
        field_snippets = self.filter_ref.resource_ref_fields()
        resource_ref_snippet = self.resource_ref.resource_ref_snippet()
        return "%s_%s" % (resource_ref_snippet, "_".join(field for field in field_snippets))

    def get_resource_dependencies(self):
        deps = super(FilteredResourceRef, self).get_resource_dependencies()
        for field_name in self.filter_ref.resource_ref_fields():
            deps.add("%s.%s" % (self.spec.name, field_name))
        return deps

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

    def __repr__(self):
        return "F[%s %s]" % (self.resource_ref, self.filter_ref)

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        return self.resource_ref.create_aggregation(user) + self.filter_ref.create_filter_aggregaton()


class ConstRef(Calc):
    ALLOWED_TYPES = {int: 'int', float: 'float', str: 'str', bool: 'bool'}

    def __init__(self, tokens, parser):
        self._parser = parser
        const = tokens[0]
        if const[0] == '"' and const[-1] == '"':
            self.value = const.strip('"')
        elif const[0] == "'" and const[-1] == "'":
            self.value = const.strip("'")
        elif const == 'true':
            self.value = True
        elif const == 'false':
            self.value = False
        else:
            try:
                self.value = int(const)
            except ValueError as v:
                self.value = float(const)
        self.const_type = ConstRef.ALLOWED_TYPES[type(self.value)]

    def infer_type(self):
        return Field('const', ConstRef.ALLOWED_TYPES[type(self.value)])

    def validate(self):
        if type(self.value) not in ConstRef.ALLOWED_TYPES:
            raise SyntaxError("Unrecognised type for const: %s" % (type(self.value)))

    def calculate(self, self_id):
        return self.value

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return []

    def __repr__(self):
        return "C[%s]" % (self.value,)

    def get_resource_dependencies(self):
        return set()

    def resource_ref_snippet(self):
        return str(hash(str(self.value))).replace('-', '_')

    def is_primitive(self):
        return True

    def _create_calc_agg_tree(self):
        return {"_v_%s" % self.resource_ref_snippet(): self}

    def _is_lookup(self):
        return False

    def _create_calc_expr(self):
        return self.value

    def create_aggregation(self, user=None):
        return [
            {"$addFields": {"_val": self.value}},
        ]


class Operator(Calc):
    def __init__(self, tokens, parser):
        self.lhs = tokens[0]
        self.op = tokens[1]
        self.rhs = tokens[2]
        self._parser = parser

    def is_primitive(self):
        return self.lhs.is_primitive()

    def root_collection(self):
        return self.lhs.root_collection()

    def aggregation(self, self_id, user=None):
        lhs_aggregation, spec, _ = self.lhs.aggregation(self_id, user)
        rhs_aggregation, _, _ = self.rhs.aggregation(self_id, user)
        aggregation = [
            {"$facet": {
                "_lhs": lhs_aggregation,
                "_rhs": rhs_aggregation,
            }},
            {"$project": {
                "_sum": {"$add": ["$_lhs", "$_rhs"]},
            }}
        ]
        return aggregation, spec, False

    def infer_type(self):
        if self.op in ('<', '>', '='):
            return Field('function', 'bool')
        else:
            return self.lhs.infer_type()

    def validate_ref(self, spec):
        if not self.lhs.infer_type().check_comparable_type(self.rhs.infer_type().field_type):
            raise SyntaxError("Illegal types for operator %s %s %s" % (self.lhs.infer_type().field_type, self.op, self.rhs.infer_type().field_type))

    def calculate(self, self_id):
        lhs = self.lhs.calculate(self_id)
        rhs = self.rhs.calculate(self_id)
        try:
            op = self.op
            if op == '+':
                return (lhs or 0) + (rhs or 0)
            elif op == '-':
                return (lhs or 0) - (rhs or 0)
            elif op == '*':
                return lhs * rhs
            elif op == '/':
                return lhs / rhs
            elif op == '>':
                return lhs > rhs
            elif op == '<':
                return lhs < rhs
            elif op == '=':
                return lhs == rhs
            elif op == '>=':
                return lhs >= rhs
            elif op == '<=':
                return lhs <= rhs
        except TypeError as te:
            return None
        except AttributeError as te:
            return None

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        lhs_aggs = self.lhs.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        rhs_aggs = self.rhs.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        lhs_aggs = lhs_aggs[1:] # remove trackers
        rhs_aggs = rhs_aggs[1:] # remove trackers
        return [[]] + lhs_aggs + rhs_aggs # add dummy tracker ( as calcs are top level )

    def get_resource_dependencies(self):
        return self.lhs.get_resource_dependencies() | self.rhs.get_resource_dependencies()

    def __repr__(self):
        return "O[%s%s%s]" % (self.lhs, self.op, self.rhs)

    def _create_calc_agg_tree(self):
        res_tree = {}
        if isinstance(self.lhs, Calc):
            res_tree.update(self.lhs._create_calc_agg_tree())
        else:
            res_tree['_lhs_%s' % self.lhs.resource_ref_snippet()] = self.lhs
        if isinstance(self.rhs, Calc):
            res_tree.update(self.rhs._create_calc_agg_tree())
        else:
            res_tree['_rhs_%s' % self.rhs.resource_ref_snippet()] = self.rhs
        return res_tree

    def _create_agg_tree(self, res_tree, user=None):
        agg_tree = {}
        for key, res in res_tree.items():
            if res._is_lookup():
                # add nested lookup
                agg_tree[key] = [
                    {"$lookup": {
                        # forced lookup to enable separate pipeline
                        "from": res.root_collection().name,
                        "as": "_lookup_val",
                        "let": {"id": "$_id"},
                        "pipeline": [
                            # match self
                            {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                        ] + res.create_aggregation(user)
                    }},
                    {"$set": {key: {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
                ]
            else:
                agg_tree[key] = res.create_aggregation(user) + [{"$addFields": {key: "$_val"}}]
        return agg_tree

    def _create_calc_expr(self):
        nullable_ops = {
            "+": "$add",
            "-": "$subtract",
        }
        ops = {
            "*": "$multiply",
            "/": "$divide",
            ">": "$gt",
            "<": "$lt",
            "=": "$eq",
            ">=": "$gte",
            "<=": "$lte",
        }

        if isinstance(self.lhs, Calc):
            lhs_expr = self.lhs._create_calc_expr()
        else:
            lhs_expr = "$_lhs_%s" % self.lhs.resource_ref_snippet()
        if isinstance(self.rhs, Calc):
            rhs_expr = self.rhs._create_calc_expr()
        else:
            rhs_expr = "$_rhs_%s" % self.rhs.resource_ref_snippet()

        if self.op in ops:
            return {
                ops[self.op]: [lhs_expr, rhs_expr]
            }
        else:
            if self.op == "+" and self.lhs.infer_type().field_type == 'str':
                return {
                    "$concat": [{"$ifNull": [lhs_expr, '']}, {"$ifNull": [rhs_expr, '']}]
                }
            else:
                return {
                    nullable_ops[self.op]: [{"$ifNull": [lhs_expr, 0]}, {"$ifNull": [rhs_expr, 0]}]
                }

    def _is_lookup(self):
        return self.lhs._is_lookup() or self.rhs._is_lookup()

    def create_aggregation(self, user=None):
        resource_tree = self._create_calc_agg_tree()
        agg_tree = self._create_agg_tree(resource_tree, user)

        aggregation = []
        for key, agg in agg_tree.items():
            aggregation.extend(agg)
        aggregation.append(
            {"$addFields": {"_val": self._create_calc_expr()}}
        )
        return aggregation


class Condition(object):
    OPERATORS = {
        '=': '$eq',
        '>': '$gt',
        '<': '$lt',
        '>=': '$gte',
        '<=': '$lte',
    }

    def validate_ref(self, spec):
        if self.field_name not in spec.fields:
            raise SyntaxError("Resource %s has no field %s" % (spec.name, self.field_name))
        field = spec.fields[self.field_name]
        if not field.check_comparable_type(self.const.infer_type().field_type):
            raise SyntaxError("Cannot compare \"%s %s %s\"" % (field.field_type, self.operator, type(self.const.calculate(None)).__name__))

    def __init__(self, tokens, parser):
        self.field_name, self.operator, self.const = tokens
        self._parser = parser

    def condition_aggregation(self, spec, resource_id):
        field_spec = spec.build_child_spec(self.field_name)
        # TODO: check removing this in favour of validate_ref()
        if not field_spec.check_comparable_type(self.const.infer_type().field_type):
            raise Exception("Incorrect type for condition: %s %s %s" %(
                self.field_name, self.operator, self.const.calculate(resource_id)))
        aggregation = {
            self.field_name: {
                self.OPERATORS[self.operator]: self.const.calculate(resource_id)}}
        return aggregation

    def __repr__(self):
        return "O[%s %s %s]" % (self.field_name, self.operator, self.const)

    def resource_ref_fields(self):
        return {self.field_name}

    def create_condition_aggregation(self):
        return {
            self.field_name: {
                self.OPERATORS[self.operator]: self.const.value
            }
        }


class LikeCondition(Condition):
    def __init__(self, tokens, parser):
        self.field_name, _, self.const = tokens
        self._parser = parser

    def condition_aggregation(self, spec, resource_id):
        field_spec = spec.build_child_spec(self.field_name)
        if type(self.const.calculate(resource_id)) is not str:
            raise Exception("Incorrect type for condition: %s ~ %s" %(
                self.field_name, self.const.calculate(resource_id)))
        aggregation = {
            self.field_name: {'$regex': self.const.calculate(resource_id), '$options': 'i'}}
        return aggregation

    def __repr__(self):
        return "O[%s ~ %s]" % (self.field_name, self.const)

    def resource_ref_fields(self):
        return {self.field_name}

    def create_condition_aggregation(self):
        return {
            self.field_name: {'$regex': self.const.value, '$options': 'i'}
        }


class AndCondition(Condition):
    def __init__(self, tokens, parser):
        self.lhs, _, self.rhs = tokens
        self._parser = parser

    def condition_aggregation(self, spec, resource_id):
        return {"$and": [self.lhs.condition_aggregation(spec, resource_id),
                         self.rhs.condition_aggregation(spec, resource_id)]}

    def __repr__(self):
        return "%s & %s" % (self.lhs, self.rhs)

    def resource_ref_fields(self):
        return {self.lhs.field_name, self.rhs.field_name}

    def create_condition_aggregation(self):
        return {"$and": [self.lhs.create_condition_aggregation(),
                         self.rhs.create_condition_aggregation()]}


class OrCondition(Condition):
    def __init__(self, tokens, parser):
        self.lhs, _, self.rhs = tokens
        self._parser = parser

    def condition_aggregation(self, spec, resource_id):
        return {"$or": [self.lhs.condition_aggregation(spec, resource_id),
                         self.rhs.condition_aggregation(spec, resource_id)]}

    def __repr__(self):
        return "%s & %s" % (self.lhs, self.rhs)

    def resource_ref_fields(self):
        return {self.lhs.field_name, self.rhs.field_name}

    def create_condition_aggregation(self):
        return {"$or": [self.lhs.create_condition_aggregation(),
                         self.rhs.create_condition_aggregation()]}


class Filter(object):
    def __init__(self, tokens, parser):
        self.condition = tokens[1]
        self._parser = parser

    def validate_ref(self, spec):
        self.condition.validate_ref(spec)

    def __repr__(self):
        return "[%s]" % (self.condition,)

    def filter_aggregation(self, spec, resource_id):
        agg = self.condition.condition_aggregation(spec, resource_id)
        aggregation = {"$match": agg}
        return aggregation

    def resource_ref_fields(self):
        return self.condition.resource_ref_fields()

    def create_filter_aggregaton(self):
        return [{"$match": self.condition.create_condition_aggregation()}]


class ResourceRefTernary(ResourceRef):
    def __init__(self, tokens, parser, spec):
        self.condition = tokens[0]
        self.then_clause = tokens[2]
        self.else_clause = tokens[4]
        self.spec = spec
        self._parser = parser

    def validate(self):
        self.condition.validate_ref(self.spec)
        self.then_clause.validate()
        self.else_clause.validate()
        if not self.then_clause.infer_type().check_comparable_type(self.else_clause.infer_type().field_type):
            # TODO: check also if both sides are collection / not collection
            raise SyntaxError("Both sides of ternary must return same type (%s != %s)" % (
                self.then_clause.infer_type().field_type,
                self.else_clause.infer_type().field_type))

    def infer_type(self):
        return self.then_clause.infer_type()

    def is_collection(self):
        return self.then_clause.is_collection()

    def __repr__(self):
        return "%s => %s : %s" % (self.condition, self.then_clause, self.else_clause)

    def aggregation(self, self_id, user=None):

        if self.condition.calculate(self_id):
            return self.then_clause.aggregation(self_id, user)
        else:
            return self.else_clause.aggregation(self_id, user)

    def calculate(self, self_id):
        if self.condition.calculate(self_id):
            return self.then_clause.calculate(self_id)
        else:
            return self.else_clause.calculate(self_id)

    def root_collection(self):
        return self.then_clause.root_collection()

    def get_resource_dependencies(self):
        deps = set()
        deps = deps.union(self.condition.get_resource_dependencies())
        deps = deps.union(self.then_clause.get_resource_dependencies())
        deps = deps.union(self.else_clause.get_resource_dependencies())
        return deps

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        condition_aggs = self.condition.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        then_aggs = self.then_clause.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        else_aggs = self.else_clause.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

#        condition_aggs = condition_aggs[1:] # remove trackers
#        then_aggs = then_aggs[1:] # remove trackers
#        else_aggs = else_aggs[1:] # remove trackers

        return [[]] + condition_aggs + then_aggs + else_aggs # add dummy tracker ( as calcs are top level )

    def create_aggregation(self, user=None):
        return [
            {"$facet": {
                "_then": [
                    {"$match": self.condition.create_condition_aggregation()},
                ] + self.then_clause.create_aggregation(user),
                "_else": [
                    {"$match": {"$not": self.condition.create_condition_aggregation()}},
                ] + self.else_clause.create_aggregation(user),
            }},
            {"$addFields": {"_val": {
                "$cond": {
                    "if": self.condition.create_condition_aggregation(),
                    "then": "$_then",
                    "else": "$_else",
                }
            }}},
        ]


class KeyValue(object):
    def __init__(self, tokens, parser):
        self._parser = parser
        self.key = tokens[0]
        self.value = tokens[2]

    def __repr__(self):
        return "%s: %s" % (self.key, self.value)


class Map(object):
    def __init__(self, tokens, parser):
        self._parser = parser
        if type(tokens[0]) is Map:
            self.keyvalues = tokens[0].keyvalues
            self.keyvalues.append(tokens[2])
        else:
            self.keyvalues = [tokens[0], tokens[2]]

    def __repr__(self):
        return "  %s  " % (self.keyvalues,)


class SwitchRef(ResourceRef):
    def __init__(self, tokens, parser, spec):
        self.resource_ref = tokens[0]
        self.spec = spec
        self._parser = parser

        if isinstance(tokens[3], KeyValue):
            self.cases = [tokens[3]]
        else:
            self.cases = [p for p in tokens[3].keyvalues]

    def validate(self):
        self.resource_ref.validate_ref(self.spec)
        for key, value in self.cases.items():
            self.value.validate_ref(self.spec)
        # validate all cases return same type
        # TODO: check also if all cases are collection / not collection
        for previous, case in zip(self.cases, self.cases[1:]):
            if not previous.infer_type().check_comparable_type(case.infer_type().field_type):
                raise SyntaxError("All cases in switch statement must be same type (%s != %s)" % (previous.infer_type().field_type, case.infer_type().field_type))

    def infer_type(self):
        return self.cases[0].value.infer_type()

    def is_collection(self):
        return self.cases[0].value.is_collection()

    def is_primitive(self):
        return self.cases[0].value.is_primitive()

    def __repr__(self):
        return "%s => %s" % (self.resource_ref, self.cases)

    def calculate(self, self_id):
        comparable_value = self.resource_ref.calculate(self_id)

        for case in self.cases:
            if comparable_value == case.key.value:
                return case.value.calculate(self_id)
        return None

    def get_resource_dependencies(self):
        deps = set()
        deps = deps.union(self.resource_ref.get_resource_dependencies())
        for case in self.cases:
            deps = deps.union(case.value.get_resource_dependencies())
        return deps

    def infer_type(self):
        return self.cases[0].value.infer_type()

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        # TODO: check "trackers"
        field_aggs = self.resource_ref.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
#        field_aggs = field_aggs[1:]

        aggregations = []
        aggregations.extend(field_aggs)
        for case in self.cases:
            case_aggs = case.value.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
#            case_aggs = case_aggs[1:]
            aggregations.extend(case_aggs)
        return aggregations

    def aggregation(self, self_id, user=None):
        comparable_value = self.resource_ref.calculate(self_id)

        for case in self.cases:
            if comparable_value == case.key.value:
                return case.value.aggregation(self_id, user)
        return [], None, False

    def create_aggregation(self, user=None):
        branches = []
        aggregation = []

        if self.resource_ref._is_lookup():
            aggregation += [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": self.resource_ref.root_collection().name,
                    "as": "_switch_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                    ] + self.resource_ref.create_aggregation(user)
                }},
            ]
            # switch val must always be primitive
            aggregation += [
                {"$addFields": {"_switch_val": {"$arrayElemAt": ["$_switch_lookup_val._val", 0]}}},
            ]
        else:
            aggregation += self.resource_ref.create_aggregation(user)
            aggregation.append({"$addFields": {"_switch_val": "$_val"}})

        for index, case in enumerate(self.cases):

            agg = []
            if case.value._is_lookup():
                # add nested lookup
                agg+= [
                    {"$lookup": {
                        # forced lookup to enable separate pipeline
                        "from": case.value.root_collection().name,
                        "as": "_case_lookup_val",
                        "let": {"id": "$_id"},
                        "pipeline": [
                            # match self
                            {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                        ] + case.value.create_aggregation(user)
                    }},
                ]
                if case.value.is_collection():
                    if case.value.is_primitive():
                        agg+= [
                            {"$addFields": {"_case_%s" % index: "$_case_lookup_val._val"}},
                        ]
                    else:
                        agg+= [
                            {"$addFields": {"_case_%s" % index: "$_case_lookup_val"}},
                        ]
                else:
                    if case.value.is_primitive():
                        agg+= [
                            {"$addFields": {"_case_%s" % index: {"$arrayElemAt": ["$_case_lookup_val._val", 0]}}},
                        ]
                    else:
                        agg+= [
                            {"$addFields": {"_case_%s" % index: {"$arrayElemAt": ["$_case_lookup_val", 0]}}},
                        ]
            else:
                agg += case.value.create_aggregation(user)
                agg.append({"$addFields": {"_case_%s" % index: "$_val"}})

            aggregation += agg

            branches.append({
                "case": {"$eq": [case.key.value, "$_switch_val"]},
                "then": "$_case_%s" % index,
            })

        aggregation.extend([
            {"$addFields": {"_val": {
                "$switch": {
                    "branches": branches,
                    "default": None
                }
            }}},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$addFields": {"_val": "$_id"}},
#            {"$replaceRoot": {"newRoot": "$_id"}},
        ])
        return aggregation


class CalcTernary(Calc):
    def __init__(self, tokens, parser, spec):
        self.condition = tokens[0]
        self.then_clause = tokens[2]
        self.else_clause = tokens[4]
        self.spec = spec
        self._parser = parser

    def get_resource_dependencies(self):
        deps = set()
        deps = deps.union(self.condition.get_resource_dependencies())
        deps = deps.union(self.then_clause.get_resource_dependencies())
        deps = deps.union(self.then_clause.get_resource_dependencies())
        return deps

    def validate(self):
        self.condition.validate_ref(self.spec)
        self.then_clause.validate()
        self.else_clause.validate()
        if not self.then_clause.infer_type().check_comparable_type(self.else_clause.infer_type().field_type):
            raise SyntaxError("Both sides of ternary must return same type (%s != %s)" % (
                self.then_clause.infer_type().field_type,
                self.else_clause.infer_type().field_type))

    def infer_type(self):
        return self.then_clause.infer_type()

    def calculate(self, self_id):
        if self.condition.calculate(self_id):
            return self.then_clause.calculate(self_id)
        else:
            return self.else_clause.calculate(self_id)

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        cond_aggs = self.condition.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        then_aggs = self.then_clause.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        else_aggs = self.else_clause.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)
        cond_aggs = cond_aggs[1:] # remove trackers
        then_aggs = then_aggs[1:] # remove trackers
        else_aggs = else_aggs[1:] # remove trackers
        return [[]] + cond_aggs + then_aggs + else_aggs # add dummy tracker ( as calcs are top level )

    def __repr__(self):
        return "%s => %s : %s" % (self.condition, self.then_clause, self.else_clause)

    def is_primitive(self):
        return self.then_clause.is_primitive()

    def _is_lookup(self):
        return True

    def create_aggregation(self, user=None):
        aggregation = []

        aggregation += self.condition.create_aggregation(user)
        aggregation.append({"$addFields": {"_if": "$_val"}})

        if self.then_clause._is_lookup():
            # add nested lookup
            aggregation += [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": self.then_clause.root_collection().name,
                    "as": "_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                    ] + self.then_clause.create_aggregation(user)
                }},
                {"$set": {"_then": {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
            ]
        else:
            aggregation += self.then_clause.create_aggregation(user)
            aggregation += [{"$addFields": {"_then": "$_val"}}]

        if self.else_clause._is_lookup():
            # add nested lookup
            aggregation += [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": self.else_clause.root_collection().name,
                    "as": "_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                    ] + self.else_clause.create_aggregation(user)
                }},
                {"$set": {"_else": {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
            ]
        else:
            aggregation += self.else_clause.create_aggregation(user)
            aggregation += [{"$addFields": {"_else": "$_val"}}]

        aggregation.append(
            {"$addFields": {"_val": {
                "$cond": {
                    "if": "$_if",
                    "then": "$_then",
                    "else": "$_else",
                }
            }}},
        )
        return aggregation


class ParameterList(object):
    def __init__(self, tokens, parser):
        self._parser = parser
        if type(tokens[0]) is ParameterList:
            self.params = tokens[0].params
            self.params.append(tokens[2])
        else:
            self.params = [tokens[0], tokens[2]]

    def __repr__(self):
        return "  %s  " % (self.params,)


class Brackets(Calc):
    def __init__(self, tokens, parser):
        self.calc = tokens[1]
        self._parser = parser

    def calculate(self, self_id):
        return self.calc.calculate(self_id)

    def aggregation(self, self_id, user):
        return self.calc.aggregation(self_id, user)

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return self.calc.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

    def __repr__(self):
        return "(" + str(self.calc) + ")"

    def infer_type(self):
        return self.calc.infer_type()

    def get_resource_dependencies(self):
        return self.calc.get_resource_dependencies()

    def is_collection(self):
        return self.calc.is_collection()

    def root_collection(self):
        return self.calc.root_collection()

    def is_primitive(self):
        return self.calc.is_primitive()

    def _create_calc_agg_tree(self):
        return self.calc._create_calc_agg_tree()

    def _create_calc_expr(self):
        return self.calc._create_calc_expr()

    def _is_lookup(self):
        return self.calc._is_lookup()

    def create_aggregation(self, user=None):
        return self.calc.create_aggregation(user)


class FunctionCall(ResourceRef):
    def __init__(self, tokens, parser):
        self.func_name = tokens[0]
        self._parser = parser

        if isinstance(tokens[1], Brackets):
            self.params = [tokens[1].calc]
        else:
            self.params = [p for p in tokens[2].params]

        self.functions = {
            'round': self._round,
            'max': self._max,
            'min': self._min,
            'average': self._average,
            'sum': self._sum,
            'days': self._days,
            'hours': self._hours,
            'minutes': self._minutes,
            'seconds': self._seconds,
        }

    def get_resource_dependencies(self):
        deps = set()
        for param in self.params:
            deps = deps.union(param.get_resource_dependencies())
        return deps

    # todo: this function needs to return a list or set for multiple params
    def resource_ref_snippet(self):
        return self.params[0].resource_ref_snippet()

    def infer_type(self):
        return Field('function', 'float')

    def calculate(self, self_id):
        return self.functions[self.func_name](self_id, *self.params)

    def _days(self, self_id, days):
        return timedelta(days=days.value)

    def _hours(self, self_id, hours):
        return timedelta(hours=hours.value)

    def _minutes(self, self_id, minutes):
        return timedelta(minutes=minutes.value)

    def _seconds(self, self_id, seconds):
        return timedelta(seconds=seconds.value)

    def _round(self, self_id, aggregate_field, digits=None):
        value = aggregate_field.calculate(self_id)

        if digits:
            return round(value, digits.calculate(self_id))
        else:
            return round(value)

    def _max(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_max': {'$max': '$' + spec.name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection().aggregate(aggregate_query)
            return cursor.next()['_max']
        except StopIteration:
            return None

    def _min(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_min': {'$min': '$' + spec.name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection().aggregate(aggregate_query)
            return cursor.next()['_min']
        except StopIteration:
            return None

    def _average(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_average': {'$avg': '$' + spec.name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection().aggregate(aggregate_query)
            return cursor.next()['_average']
        except StopIteration:
            return None

    def _sum(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_sum': {'$sum': '$' + aggregate_field.field_name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection().aggregate(aggregate_query)
            return cursor.next()['_sum']
        except StopIteration:
            return None

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        # take into account different functions and params
        return self.params[0].build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

    def __repr__(self):
        return "%s(%s)" % (self.func_name, [str(s) for s in self.params])

    def _create_calc_agg_tree(self):
        return {"_v_%s" % self.resource_ref_snippet(): self}

    def _is_lookup(self):
        return self.params[0]._is_lookup()

    def is_primitive(self):
        functions = {
            'round': True,
            'max': True,
            'min': True,
            'average': True,
            'sum': True,
            'days': True,
            'hours': True,
            'minutes': True,
            'seconds': True,
            'first': False,
        }
        return functions[self.func_name]

    def is_collection(self):
        return False

    def create_aggregation(self, user=None):
        functions = {
            'round': self._agg_round,
            'max': self._agg_max,
            'min': self._agg_min,
            'average': self._agg_average,
            'sum': self._agg_sum,
            'days': self._agg_days,
            'hours': self._agg_hours,
            'minutes': self._agg_minutes,
            'seconds': self._agg_seconds,
            'first': self._agg_first,
        }
        return functions[self.func_name](user, *self.params)

    def root_collection(self):
        return self.params[0].root_collection()

    def _agg_round(self, user, field, digits=None):
        # validate for constant for digits
        digits = digits.value if digits else 0
        round_agg = ["$_val"] + [digits]
        return field.create_aggregation(user) + [
            {"$addFields": {"_val": {"$round": round_agg}}}
        ]

    def _agg_max(self, user, collection):
        return collection.create_aggregation(user) + [
            {'$group': {'_id': None, '_val': {'$max': '$' + collection.field_name}}}
        ]

    def _agg_min(self, user, collection):
        return collection.create_aggregation(user) + [
            {'$group': {'_id': None, '_val': {'$min': '$' + collection.field_name}}}
        ]

    def _agg_average(self, user, agg_field):
        return agg_field.create_aggregation(user) + [
            {'$group': {'_id': None, '_val': {'$avg': '$' + agg_field.field_name}}}
        ]

    def _agg_sum(self, user, agg_field):
        return agg_field.create_aggregation(user) + [
            {'$group': {'_id': None, '_val': {'$sum': '$' + agg_field.field_name}}}
        ]

    def _agg_days(self, user, field):
        return field.create_aggregation(user) + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000, 60, 60, 24]}}}
        ]

    def _agg_hours(self, user, field):
        return field.create_aggregation(user) + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000, 60, 60]}}}
        ]

    def _agg_minutes(self, user, field):
        return field.create_aggregation(user) + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000, 60]}}}
        ]

    def _agg_seconds(self, user, field):
        return field.create_aggregation(user) + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000]}}}
        ]

    def _agg_first(self, user, collection):
        return collection.create_aggregation(user) + [
            {"$set": {"_val": {"$arrayElemAt": ["$_val", 0]}}},
        ]


NAME = 'NAME'
STRING = 'STRING'
NUMBER = 'NUMBER'
ID = 'ID'

def lex(raw_tokens):
    ignore_list = [
        tokenize.NEWLINE,
        tokenize.COMMENT,
        tokenize.Whitespace,
    ]
    tokens = []
    for token_type, value, line, col, _ in raw_tokens:
        if token_type == tokenize.OP:
            tokens.append((value, value))
        elif token_type == tokenize.STRING:
            tokens.append((STRING, value))
        elif token_type == tokenize.NUMBER:
            tokens.append((NUMBER, value))
        elif token_type == tokenize.NAME:
            if len(value) == 26 and value[:2] == 'ID':
                tokens.append((ID, value))
            else:
                tokens.append((NAME, value))
        elif value in ignore_list:
            pass
        elif token_type == tokenize.ENDMARKER:
            pass
        elif token_type == tokenize.NEWLINE:
            pass
        elif token_type == tokenize.COLON:
            pass
        else:
            raise Exception("Unexpected token [%s] at line %s col %s" % (
                            value, line, col))
    return tokens


class Parser(object):

    def __init__(self, tokens, spec):
        self.tokens = tokens
        self.spec = spec
        self.shifted = []
        self.patterns = [
            [(NAME, '=', ConstRef) , Condition],
            [(NAME, '>', ConstRef) , Condition],
            [(NAME, '<', ConstRef) , Condition],
            [(NAME, '>=', ConstRef) , Condition],
            [(NAME, '<=', ConstRef) , Condition],

            [(NAME, '=', ResourceRef) , Condition],
            [(NAME, '>', ResourceRef) , Condition],
            [(NAME, '<', ResourceRef) , Condition],
            [(NAME, '>=', ResourceRef) , Condition],
            [(NAME, '<=', ResourceRef) , Condition],

            [(Operable, '+', Operable) , Operator],
            [(Operable, '-', Operable) , Operator],
            [(Operable, '*', Operable) , Operator],
            [(Operable, '/', Operable) , Operator],
            [(Operable, '>', Operable) , Operator],
            [(Operable, '<', Operable) , Operator],
            [(Operable, '=', Operable) , Operator],

            [(NAME, '~', ConstRef) , LikeCondition],
            [(Calc, '>=', Calc) , Operator],
            [(Calc, '<=', Calc) , Operator],
            [(Condition, '&', Condition) , AndCondition],
            [(Condition, '|', Condition) , OrCondition],
            [('[', Condition, ']'), Filter],
            [(Calc, ',', Calc), ParameterList],
            [(Calc, ',', ResourceRef), ParameterList],
            [(ResourceRef, ',', ResourceRef), ParameterList],
            [(ResourceRef, ',', Calc), ParameterList],
            [(ParameterList, ',', Calc), ParameterList],
            [(ParameterList, ',', ResourceRef), ParameterList],
            [('(', Calc, ')'), Brackets],
            [('(', ResourceRef, ')'), Brackets],
            [(NAME, Brackets), FunctionCall],
            [(NAME, '(', ParameterList, ')'), FunctionCall],
            [(NAME, '(', NAME, ')'), FunctionCall],
            [(STRING,), ConstRef],
            [(NUMBER,), ConstRef],
            [(ResourceRef, Filter), self._create_filtered_resource_ref],
            [(NAME, Filter), self._create_filtered_resource_ref],
            [(ResourceRef, '.', NAME), self._create_resource_ref],
            [(NAME, '.', NAME), self._create_resource_ref],

            [(Operator, '->', Calc, ':', Calc), self._create_calc_ternary],
            [(Operator, '->', ResourceRef, ':', ResourceRef), self._create_calc_ternary],
            [(Operator, '->', ResourceRef, ':', Calc), self._create_calc_ternary],
            [(Operator, '->', Calc, ':', ResourceRef), self._create_calc_ternary],

            [(ConstRef, ':', Calc), KeyValue],
            [(ConstRef, ':', ResourceRef), KeyValue],
            [(KeyValue, ',', KeyValue), Map],
            [(Map, ',', KeyValue), Map],
            [(FieldRef, '->', '(', Map, ')'), self._create_switch],
            [(FieldRef, '->', '(', KeyValue, ')'), self._create_switch],
        ]

    def _create_resource_ref(self, tokens, parser):
        if isinstance(tokens[0], str):
            root_resource_ref = RootResourceRef(tokens[0], parser, self.spec)
        else:
            root_resource_ref = tokens[0]

        field_name = tokens[2]
        child_spec = root_resource_ref.spec.build_child_spec(field_name)
        if root_resource_ref.spec.fields[field_name].field_type == 'collection':
            return CollectionResourceRef(root_resource_ref, field_name, parser, child_spec, root_resource_ref.spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'linkcollection':
            return LinkCollectionResourceRef(root_resource_ref, field_name, parser, child_spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'orderedcollection':
            return OrderedCollectionResourceRef(root_resource_ref, field_name, parser, child_spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'link':
            return LinkResourceRef(root_resource_ref, field_name, parser, child_spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'calc':
            return CalcResourceRef(root_resource_ref, field_name, parser, child_spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'reverse_link_collection':
            return ReverseLinkCollectionResourceRef(root_resource_ref, field_name, parser, child_spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'reverse_link':
            return ReverseLinkResourceRef(root_resource_ref, field_name, parser, child_spec)
        if root_resource_ref.spec.fields[field_name].field_type == 'parent_collection':
            return ParentCollectionResourceRef(root_resource_ref, field_name, parser, child_spec)
        if child_spec.is_field():
            return FieldRef(root_resource_ref, field_name, parser, child_spec)
        return ResourceRef(root_resource_ref, field_name, parser, child_spec)

    def _create_ternary(self, tokens, parser):
        return ResourceRefTernary(tokens, parser, self.spec)

    def _create_switch(self, tokens, parser):
        return SwitchRef(tokens, parser, self.spec)

    def _create_calc_ternary(self, tokens, parser):
        return CalcTernary(tokens, parser, self.spec)

    def _create_filtered_resource_ref(self, tokens, parser):
        if isinstance(tokens[0], str):
            root_resource_ref = RootResourceRef(tokens[0], parser, self.spec)
        else:
            root_resource_ref = tokens[0]

        filter_ref = tokens[1]
        return FilteredResourceRef(root_resource_ref, filter_ref, parser, root_resource_ref.spec)

    def _create_id_resource_ref(self, tokens, parser):
        if isinstance(tokens[0], str):
            root_resource_ref = RootResourceRef(tokens[0], parser, self.spec)
        else:
            root_resource_ref = tokens[0]

        resource_id = tokens[2]
        return IDResourceRef(root_resource_ref, resource_id, parser, root_resource_ref.spec)

    def match_pattern(self, pattern, last_tokens):
        def m(pat, tok):
            return pat == tok[0] or (not isinstance(tok[0], str) and not isinstance(pat, str) and issubclass(tok[0], pat))
        return len(pattern) == len(last_tokens) and all(m(pat, tok) for pat, tok in zip(pattern, last_tokens))

    def match_and_reduce(self):
        for pattern, reduced_class in self.patterns:
            last_shifted = self.shifted[-len(pattern):]
#            print ("Checking %s against %s" % (pattern,last_shifted))
            if self.match_pattern(pattern, last_shifted):
#                print ("** Reducing %s(%s)" % (reduced_class, last_shifted))
                self._reduce(reduced_class, last_shifted)
#                print ("** shifted: %s" % (self.shifted,))
                return True

        return False

    def parse(self):
        while self.tokens:
            self.shifted.append(self.tokens.pop(0))
            while self.match_and_reduce():
                pass

        if len(self.shifted) > 1:
            raise Exception("Unexpected '%s'" % (self.shifted[1],))

        if self.shifted[0][0] == 'NAME' and self.shifted[0][1] in self.spec.schema.root.fields:
            return RootResourceRef(self.shifted[0][1], self, self.spec)

        if self.shifted[0][1] == 'self':
            raise Exception("Calc cannot be 'self' only.")

        if self.shifted[0][1] == 'ego':
            return RootResourceRef('ego', self, self.spec)

        tree = self.shifted[0][1]

        if type(tree) is str:
            raise SyntaxError('Cannot parse expression: %s' % tree)

        return tree

    def _reduce(self, reduced_class, tokens):
        self.shifted = self.shifted[:-len(tokens)]
        reduction = reduced_class([a[1] for a in tokens], self)
        self.shifted.append((type(reduction), reduction))


class UrlParser(Parser):
    def __init__(self, tokens, spec):
        self.tokens = tokens
        self.spec = spec
        self.shifted = []
        self.patterns = [
            [(NAME, '=', ConstRef) , Condition],
            [(NAME, '>', ConstRef) , Condition],
            [(NAME, '<', ConstRef) , Condition],
            [(NAME, '~', ConstRef) , LikeCondition],
            [(NAME, '>=', ConstRef) , Condition],
            [(NAME, '<=', ConstRef) , Condition],

            [(Condition, '&', Condition) , AndCondition],
            [(Condition, '|', Condition) , OrCondition],
            [('[', Condition, ']'), Filter],
            [(STRING,), ConstRef],
            [(NUMBER,), ConstRef],
            [(ResourceRef, Filter), self._create_filtered_resource_ref],
            [(NAME, Filter), self._create_filtered_resource_ref],
            [(ResourceRef, '/', NAME), self._create_resource_ref],
            [(NAME, '/', NAME), self._create_resource_ref],
            [(ResourceRef, '/', ID), self._create_id_resource_ref],
            [(NAME, '/', ID), self._create_id_resource_ref],
        ]


class CanonicalUrlParser(Parser):
    def __init__(self, tokens, spec):
        self.tokens = tokens
        self.spec = spec
        self.shifted = []
        self.patterns = [
            [(ResourceRef, '/', NAME), self._create_resource_ref],
            [(NAME, '/', NAME), self._create_resource_ref],
            [(ResourceRef, '/', ID), self._create_id_resource_ref],
            [(NAME, '/', ID), self._create_id_resource_ref],
        ]


class FilterParser(Parser):
    def __init__(self, tokens, spec):
        self.tokens = tokens
        self.spec = spec
        self.shifted = []
        self.patterns = [
            [(NAME, '=', ConstRef) , Condition],
            [(NAME, '>', ConstRef) , Condition],
            [(NAME, '<', ConstRef) , Condition],
            [(NAME, '~', ConstRef) , LikeCondition],
            [(NAME, '>=', ConstRef) , Condition],
            [(NAME, '<=', ConstRef) , Condition],

            [(Condition, '&', Condition) , AndCondition],
            [(Condition, '|', Condition) , OrCondition],
            [(Condition, ',', Condition) , OrCondition],
            [('[', Condition, ']'), Filter],
            [(STRING,), ConstRef],
            [(NUMBER,), ConstRef],
        ]


def parse(line, spec):
    line = line.replace('\n', ' ').replace('\t', ' ')
    tokens = tokenize.generate_tokens(StringIO(line).read)
    return Parser(lex(tokens), spec).parse()


def parse_url(line, spec):
    line = line.replace('\n', ' ').replace('\t', ' ')
    tokens = tokenize.generate_tokens(StringIO(line).read)
    return UrlParser(lex(tokens), spec).parse()


def parse_canonical_url(line, spec):
    line = line.replace('\n', ' ').replace('\t', ' ')
    tokens = tokenize.generate_tokens(StringIO(line).read)
    return CanonicalUrlParser(lex(tokens), spec).parse()


def parse_filter(line, spec):
    line = line.replace('\n', ' ').replace('\t', ' ')
    tokens = tokenize.generate_tokens(StringIO(line).read)
    return FilterParser(lex(tokens), spec).parse()
