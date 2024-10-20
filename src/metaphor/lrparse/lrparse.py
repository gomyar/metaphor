
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

    def create_aggregation(self):
        return self.resource_ref.create_aggregation()


class FieldRef(ResourceRef, Calc):
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

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
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
        if self.resource_name == "ego":
            raise Exception("Unexpected ego reference in reverse aggregation")
        if self.resource_name in ["self"]:
            #return [[], []]
            return [[], [{"$match": {"_type": calc_spec_name}}]]
            #return [[]]
        else:
            # assuming root / collection
            return [[], self.create_reverse(calc_spec_name, calc_field_name)]

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                "from": "metaphor_resource",
                "as": "_field_%s" % (calc_field_name,),
                "pipeline": [{"$match": {"_type": calc_spec_name}}],
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
        return self.spec.schema.db['metaphor_resource']

    def get_resource_dependencies(self):
        if self.resource_name == 'self':
            return set()
        else:
            return {"root.%s" % self.resource_name}

    def infer_type(self):
        return self.spec

    def is_collection(self):
        if self.resource_name in ('self', 'ego'):
            return False
        else:
            return True

    def __repr__(self):
        return "T[%s]" % (self.resource_name,)

    def resource_ref_snippet(self):
        return self.resource_name

    def _is_lookup(self):
        return self.resource_name != 'self'

    def create_aggregation(self):
        if self.resource_name == 'self':
            return []
        elif self.resource_name == 'ego':
            return []
        else:
            return [
                {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_val",
                    "pipeline":[
                        {"$match": {
                            "_type": self.spec.name,
                            "_parent_field_name": self.resource_name,
                            "_parent_canonical_url": '/',
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

    def is_collection(self):
        return False

    def __repr__(self):
        return "I[%s.%s]" % (self.resource_ref, self.resource_id,)

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id, calc_spec_name, calc_field_name)

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$match": {
                "_id": self.spec.schema.decodeid(self.resource_id),
                "_deleted": {"$exists": False},
            }}
        ]


class CollectionResourceRef(ResourceRef):
    def __init__(self, resource_ref, field_name, parser, spec, parent_spec):
        super(CollectionResourceRef, self).__init__(resource_ref, field_name, parser, spec)
        self.parent_spec = parent_spec

    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                "from": "metaphor_resource",
                "as": "_field_%s" % (self.field_name,),
                "let": {"id": "$_parent_id"},
                "pipeline": [
                    {"$match": {"$expr":
                        {"$and": [
                            {"$eq": ["$_id", "$$id"]},
                            {"$eq": ["$_type", self.parent_spec.name]},
                        ]}
                    }}
                ]
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_parent_id", "$$id"]},
                                    {"$eq": ["$_type", self.spec.name]},
                                ]
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
    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_field_%s" % (self.field_name,),
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    #{"$eq": ["$%s._id" % self.field_name, "$$id"]},
                                    {"$in": [{"_id": "$$id"}, {"$ifNull": ["$%s" % self.field_name, []]}]},
                                    {"$eq": ["$_type", self.resource_ref.spec.name]},
                                ]
                            }
                        }}
                    ]
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

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_val",
                    "let": {"id": {"$ifNull": ["$%s" % self.field_name, []]}},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$in": [{"_id": "$_id"}, "$$id"]},
                                    {"$eq": ["$_type", self.spec.name]},
                                ]
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
    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_field_%s" % (self.field_name,),
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$%s" % self.field_name, "$$id"]},
                                    {"$eq": ["$_type", self.resource_ref.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def _is_lookup(self):
        return True

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$lookup": {
                "from": 'metaphor_resource',
                "as": '_val',
                "let": {"id": "$%s" % self.field_name},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$_id", "$$id"]},
                                {"$eq": ["$_type", self.spec.name]},
                            ]
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
    def is_collection(self):
        spec = self.resource_ref.infer_type()
        calc_tree = spec.schema.calc_trees[spec.name, self.field_name]
        return calc_tree.is_collection()

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_field_%s" % (self.field_name,),
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$%s" % self.field_name, "$$id"]},
                                    {"$eq": ["$_type", self.resource_ref.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _create_calc_agg_tree(self):
        return {"_v_%s" % self.field_name: self}

    def _create_calc_expr(self):
        return "$%s" % self.field_name

    def create_aggregation(self):
        agg = self.resource_ref.create_aggregation()
        calc_tree = self.resource_ref.spec.schema.calc_trees[self.resource_ref.spec.name, self.field_name]
        calc_spec = calc_tree.infer_type()
        if calc_tree.is_primitive():
            return agg + [
                {"$addFields": {"_val": "$%s" % self.field_name}},
            ]
        else:
            return agg + [
                {"$lookup": {
                        "from": "metaphor_resource",
                        "localField": "%s._id" % self.field_name,
                        "foreignField": "_id",
                        "as": "_val",
                }},
                {'$group': {'_id': '$_val'}},
                {"$unwind": "$_id"},
                {"$replaceRoot": {"newRoot": "$_id"}},
            ]


class ReverseLinkResourceRef(ResourceRef):
    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        #_, reverse_spec, reverse_field = self.field_name.split('_')  # well this should have a better impl
        reverse_spec = self.spec.name
        reverse_field = self.resource_ref.spec.name
        return {"%s.%s" % (reverse_spec, reverse_field)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_field_%s" % (self.field_name,),
                    "let": {"id": "$%s" % self.resource_ref.spec.fields[self.field_name].reverse_link_field},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_id", "$$id"]},
                                    {"$eq": ["$_type", self.resource_ref.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$$id", "$%s" % self.resource_ref.spec.fields[self.field_name].reverse_link_field]},
                                    {"$eq": ["$_type", self.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
            # TODO: match _deleted: false ??
        ]


class ParentCollectionResourceRef(ResourceRef):
    def is_collection(self):
        return False

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_field_%s" % (self.field_name,),
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_parent_id", "$$id"]},
                                    {"$eq": ["$_type", self.resource_ref.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_val",
                    "let": {"id": "$_parent_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_id", "$$id"]},
                                    {"$eq": ["$_type", self.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
            # TODO: match _deleted: false ??
        ]


class ReverseLinkCollectionResourceRef(ResourceRef):
    def is_collection(self):
        return True

    def get_resource_dependencies(self):
        _, reverse_spec, reverse_field = self.field_name.split('_')  # well this should have a better impl
        return {"%s.%s" % (reverse_spec, reverse_field)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self, calc_spec_name, calc_field_name):
        return [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_field_%s" % (self.field_name,),
                    "let": {"id": {"$ifNull": ["$%s" % self.resource_ref.spec.fields[self.field_name].reverse_link_field, []]}},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$in": [{"_id": "$_id"}, "$$id"]},
                                    {"$eq": ["$_type", self.resource_ref.spec.name]},
                                ]
                            }
                        }}
                    ]
            }},
            {'$group': {'_id': '$_field_%s' % (self.field_name,)}},
            {"$unwind": "$_id"},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ]

    def _is_lookup(self):
        return True

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + [
            {"$lookup": {
                    "from": "metaphor_resource",
                    "as": "_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$in": [{"_id": "$$id"}, {"$ifNull": ["$%s" % self.resource_ref.spec.fields[self.field_name].reverse_link_field, []]}]},
                                    {"$eq": ["$_type", self.resource_ref.spec.fields[self.field_name].target_spec_name]},
                                ]
                            }
                        }}
                    ]
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

    def create_aggregation(self):
        return self.resource_ref.create_aggregation() + self.filter_ref.create_filter_aggregaton()


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

    def build_reverse_aggregations(self, resource_spec, resource_id, calc_spec_name, calc_field_name):
        return []

    def __repr__(self):
        return "C[%s]" % (self.value,)

    def get_resource_dependencies(self):
        return set()

    def resource_ref_snippet(self):
        if self.value is True: return "true"
        if self.value is False: return "false"
        if self.value not in self._parser.constants:
            self._parser.constants.append(self.value)
        return self._parser.constants.index(self.value)

    def is_primitive(self):
        return True

    def _create_calc_agg_tree(self):
        return {"_v_%s" % self.resource_ref_snippet(): self}

    def _is_lookup(self):
        return False

    def _create_calc_expr(self):
        return self.value

    def create_aggregation(self):
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

    def aggregation(self, self_id):
        lhs_aggregation, spec, _ = self.lhs.aggregation(self_id)
        rhs_aggregation, _, _ = self.rhs.aggregation(self_id)
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

    def _create_agg_tree(self, res_tree):
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
                        ] + res.create_aggregation()
                    }},
                    {"$set": {key: {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
                ]
            else:
                agg_tree[key] = res.create_aggregation() + [{"$addFields": {key: "$_val"}}]
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
            "!=": "$ne",
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

    def create_aggregation(self):
        resource_tree = self._create_calc_agg_tree()
        agg_tree = self._create_agg_tree(resource_tree)

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
        '!=': '$ne',
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
            raise SyntaxError("Cannot compare \"%s %s %s\"" % (field.field_type, self.operator, type(self.const.value).__name__))

    def __init__(self, tokens, parser):
        self.field_name, self.operator, self.const = tokens
        self._parser = parser

    def condition_aggregation(self, spec, resource_id):
        field_spec = spec.build_child_spec(self.field_name)
        # TODO: check removing this in favour of validate_ref()
        if not field_spec.check_comparable_type(self.const.infer_type().field_type):
            raise Exception("Incorrect type for condition: %s %s %s" %(
                self.field_name, self.operator, self.value))
        aggregation = {
            self.field_name: {
                self.OPERATORS[self.operator]: self.const.value}}
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
        if type(self.const.value) is not str:
            raise Exception("Incorrect type for condition: %s ~ %s" %(
                self.field_name, self.const.value))
        aggregation = {
            self.field_name: {'$regex': self.const.value, '$options': 'i'}}
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

    def aggregation(self, self_id):

        if self.condition.calculate(self_id):
            return self.then_clause.aggregation(self_id)
        else:
            return self.else_clause.aggregation(self_id)

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

    def create_aggregation(self):
        return [
            {"$facet": {
                "_then": [
                    {"$match": self.condition.create_condition_aggregation()},
                ] + self.then_clause.create_aggregation(),
                "_else": [
                    {"$match": {"$not": self.condition.create_condition_aggregation()}},
                ] + self.else_clause.create_aggregation(),
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

    def aggregation(self, self_id):
        comparable_value = self.resource_ref.calculate(self_id)

        for case in self.cases:
            if comparable_value == case.key.value:
                return case.value.aggregation(self_id)
        return [], None, False

    def create_aggregation(self):
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
                    ] + self.resource_ref.create_aggregation()
                }},
            ]
            # switch val must always be primitive
            aggregation += [
                {"$addFields": {"_switch_val": {"$arrayElemAt": ["$_switch_lookup_val._val", 0]}}},
            ]
        else:
            aggregation += self.resource_ref.create_aggregation()
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
                        ] + case.value.create_aggregation()
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
                agg += case.value.create_aggregation()
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
        ])
        if self.cases[0].value.is_primitive():
            aggregation.append(
                {"$addFields": {"_val": "$_id"}},
            )
        else:
            aggregation.append(
                {"$replaceRoot": {"newRoot": "$_id"}},
            )
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

    def is_collection(self):
        return self.then_clause.is_collection()

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

    def create_aggregation(self):
        aggregation = []

        aggregation += self.condition.create_aggregation()
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
                    ] + self.then_clause.create_aggregation()
                }},
                {"$set": {"_then": "$_lookup_val"}},
            ]
        else:
            aggregation += self.then_clause.create_aggregation()
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
                    ] + self.else_clause.create_aggregation()
                }},
                {"$set": {"_else": "$_lookup_val"}},
            ]
        else:
            aggregation += self.else_clause.create_aggregation()
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

    def create_aggregation(self):
        aggregation = []

        aggregation += self.condition.create_aggregation()
        aggregation.append({"$addFields": {"_if": "$_val"}})


        # then
        agg = []
        if self.then_clause._is_lookup():
            # add nested lookup
            agg+= [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": self.then_clause.root_collection().name,
                    "as": "_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                    ] + self.then_clause.create_aggregation()
                }},
            ]
            if self.then_clause.is_collection():
                if self.then_clause.is_primitive():
                    agg+= [
                        {"$addFields": {"_then": "$_lookup_val._val"}},
                    ]
                else:
                    agg+= [
                        {"$addFields": {"_then": "$_lookup_val"}},
                    ]
            else:
                if self.then_clause.is_primitive():
                    agg+= [
                        {"$addFields": {"_then": {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
                    ]
                else:
                    agg+= [
                        {"$addFields": {"_then": {"$arrayElemAt": ["$_lookup_val", 0]}}},
                    ]
        else:
            agg += self.then_clause.create_aggregation()
            agg.append({"$addFields": {"_then": "$_val"}})

        aggregation += agg

        # else
        agg = []
        if self.else_clause._is_lookup():
            # add nested lookup
            agg+= [
                {"$lookup": {
                    # forced lookup to enable separate pipeline
                    "from": self.else_clause.root_collection().name,
                    "as": "_lookup_val",
                    "let": {"id": "$_id"},
                    "pipeline": [
                        # match self
                        {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                    ] + self.else_clause.create_aggregation()
                }},
            ]
            if self.else_clause.is_collection():
                if self.else_clause.is_primitive():
                    agg+= [
                        {"$addFields": {"_else": "$_lookup_val._val"}},
                    ]
                else:
                    agg+= [
                        {"$addFields": {"_else": "$_lookup_val"}},
                    ]
            else:
                if self.else_clause.is_primitive():
                    agg+= [
                        {"$addFields": {"_else": {"$arrayElemAt": ["$_lookup_val._val", 0]}}},
                    ]
                else:
                    agg+= [
                        {"$addFields": {"_else": {"$arrayElemAt": ["$_lookup_val", 0]}}},
                    ]
        else:
            agg += self.else_clause.create_aggregation()
            agg.append({"$addFields": {"_else": "$_val"}})

        aggregation += agg



        aggregation.extend([
            {"$addFields": {"_val": {
                "$cond": {
                    "if": "$_if",
                    "then": "$_then",
                    "else": "$_else",
                }
            }}},
            {'$group': {'_id': '$_val'}},
            {"$unwind": "$_id"},
        ])


        if self.then_clause.is_primitive():
            aggregation.append(
                {"$addFields": {"_val": "$_id"}},
            )
        else:
            aggregation.append(
                {"$replaceRoot": {"newRoot": "$_id"}},
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

    def aggregation(self, self_id):
        return self.calc.aggregation(self_id)

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

    def create_aggregation(self):
        return self.calc.create_aggregation()


class FunctionCall(ResourceRef):
    def __init__(self, tokens, parser):
        self.func_name = tokens[0]
        self._parser = parser

        if isinstance(tokens[1], Brackets):
            self.params = [tokens[1].calc]
        else:
            self.params = [p for p in tokens[2].params]

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

    def create_aggregation(self):
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
        return functions[self.func_name](*self.params)

    def root_collection(self):
        return self.params[0].root_collection()

    def _agg_round(self, field, digits=None):
        # validate for constant for digits
        digits = digits.value if digits else 0
        round_agg = ["$_val"] + [digits]
        return field.create_aggregation() + [
            {"$addFields": {"_val": {"$round": round_agg}}}
        ]

    def _agg_max(self, collection):
        return collection.create_aggregation() + [
            {'$group': {'_id': None, '_val': {'$max': '$' + collection.field_name}}}
        ]

    def _agg_min(self, collection):
        return collection.create_aggregation() + [
            {'$group': {'_id': None, '_val': {'$min': '$' + collection.field_name}}}
        ]

    def _agg_average(self, agg_field):
        return agg_field.create_aggregation() + [
            {'$group': {'_id': None, '_val': {'$avg': '$' + agg_field.field_name}}}
        ]

    def _agg_sum(self, agg_field):
        return agg_field.create_aggregation() + [
            {'$group': {'_id': None, '_val': {'$sum': '$' + agg_field.field_name}}},
        ]

    def _agg_days(self, field):
        return field.create_aggregation() + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000, 60, 60, 24]}}}
        ]

    def _agg_hours(self, field):
        return field.create_aggregation() + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000, 60, 60]}}}
        ]

    def _agg_minutes(self, field):
        return field.create_aggregation() + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000, 60]}}}
        ]

    def _agg_seconds(self, field):
        return field.create_aggregation() + [
            {"$addFields": {"_val": {"$multiply": ["$_val", 1000]}}}
        ]

    def _agg_first(self, collection):
        return collection.create_aggregation() + [
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
        self.constants = []
        self.patterns = [
            [(NAME, '=', ConstRef) , Condition],
            [(NAME, '>', ConstRef) , Condition],
            [(NAME, '<', ConstRef) , Condition],
            [(NAME, '>=', ConstRef) , Condition],
            [(NAME, '<=', ConstRef) , Condition],
            [(NAME, '!=', ConstRef) , Condition],

            [(NAME, '=', ResourceRef) , Condition],
            [(NAME, '>', ResourceRef) , Condition],
            [(NAME, '<', ResourceRef) , Condition],
            [(NAME, '>=', ResourceRef) , Condition],
            [(NAME, '<=', ResourceRef) , Condition],
            [(NAME, '!=', ResourceRef) , Condition],

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
            [(Calc, '!=', Calc) , Operator],
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
            [(NAME, '!=', ConstRef) , Condition],

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
            [(NAME, '!=', ConstRef) , Condition],

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
