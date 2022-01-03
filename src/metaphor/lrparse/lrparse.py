
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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        # create own agg
        agg = self.create_reverse()

        # get all subsequent aggs
        aggregations = self.resource_ref.build_reverse_aggregations(resource_spec, resource_id)

        # prepend own agg to initial (ignored) agg chain
        aggregations[0] = agg + aggregations[0]

        # if own spec same as changed resource spec
        if self.spec == resource_spec:
            # duplicate initial agg chain, add match, and return
            aggregations.insert(0, list(aggregations[0]))
            aggregations[1].insert(0, {"$match": {"_id": self.spec.schema.decodeid(resource_id)}})
        return aggregations

    def __repr__(self):
        return "R[%s %s]" % (self.resource_ref, self.field_name)

    def resource_ref_snippet(self):
        return self.resource_ref.resource_ref_snippet() + '.' + self.field_name

    def validate(self):
        pass


class FieldRef(ResourceRef, Calc):
    def aggregation(self, self_id, user=None):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id, user)
        child_spec = spec.build_child_spec(self.field_name)
        aggregation.append(
            {"$project": {
                self.field_name: True,
            }})
        return aggregation, child_spec, is_aggregate

    def get_resource_dependencies(self):
        return self.resource_ref.get_resource_dependencies() | {'%s.%s' % (self.spec.spec.name, self.field_name)}

    def build_reverse_aggregations(self, resource_spec, resource_id):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id)


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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        return [[]]

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
        if user:
            aggregation = [
                {"$match": {"_grants": {"$in": user.grants}}}
            ]
        else:
            aggregation = []
        if self.resource_name == 'self':
            aggregation.extend([
                {"$match": {"_id": self.spec.schema.decodeid(self_id)}}
            ])
            return aggregation, self.spec, False
        elif self.resource_name == 'ego':
            aggregation = [
                {"$match": {"username": user.username}}
            ]
            return aggregation, self.spec, False
        else:
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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id)


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

    def create_reverse(self):
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

    def create_reverse(self):
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

    def create_reverse(self):
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


class CalcResourceRef(ResourceRef):
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
            is_aggregate = is_aggregate or calc_tree.is_collection()
        return aggregation, calc_spec, is_aggregate

    def get_resource_dependencies(self):
        return {"%s.%s" % (self.resource_ref.spec.name, self.field_name)} | self.resource_ref.get_resource_dependencies()

    def create_reverse(self):
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

    def create_reverse(self):
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

    def create_reverse(self):
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

    def create_reverse(self):
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
        return {"%s.%s" % (resource_ref_snippet, field) for field in field_snippets}

    def get_resource_dependencies(self):
        deps = super(FilteredResourceRef, self).get_resource_dependencies()
        for field_name in self.filter_ref.resource_ref_fields():
            deps.add("%s.%s" % (self.spec.name, field_name))
        return deps

    def build_reverse_aggregations(self, resource_spec, resource_id):
        return self.resource_ref.build_reverse_aggregations(resource_spec, resource_id)

    def __repr__(self):
        return "F[%s %s]" % (self.resource_ref, self.filter_ref)


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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        return []

    def __repr__(self):
        return "C[%s]" % (self.value,)

    def get_resource_dependencies(self):
        return set()

class Operator(Calc):
    def __init__(self, tokens, parser):
        self.lhs = tokens[0]
        self.op = tokens[1]
        self.rhs = tokens[2]
        self._parser = parser

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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        lhs_aggs = self.lhs.build_reverse_aggregations(resource_spec, resource_id)
        rhs_aggs = self.rhs.build_reverse_aggregations(resource_spec, resource_id)
        lhs_aggs = lhs_aggs[1:] # remove trackers
        rhs_aggs = rhs_aggs[1:] # remove trackers
        return [[]] + lhs_aggs + rhs_aggs # add dummy tracker ( as calcs are top level )

    def get_resource_dependencies(self):
        return self.lhs.get_resource_dependencies() | self.rhs.get_resource_dependencies()

    def __repr__(self):
        return "O[%s%s%s]" % (self.lhs, self.op, self.rhs)


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
            raise SyntaxError("Both sides of ternary must return same type (%s != %s)" % (self.then_clause.infer_type().field_type, self.else_clause.infer_type().field_type))

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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        condition_aggs = self.condition.build_reverse_aggregations(resource_spec, resource_id)
        then_aggs = self.then_clause.build_reverse_aggregations(resource_spec, resource_id)
        else_aggs = self.else_clause.build_reverse_aggregations(resource_spec, resource_id)

#        condition_aggs = condition_aggs[1:] # remove trackers
#        then_aggs = then_aggs[1:] # remove trackers
#        else_aggs = else_aggs[1:] # remove trackers

        return [[]] + condition_aggs + then_aggs + else_aggs # add dummy tracker ( as calcs are top level )


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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        # TODO: check "trackers"
        field_aggs = self.resource_ref.build_reverse_aggregations(resource_spec, resource_id)
#        field_aggs = field_aggs[1:]

        aggregations = []
        aggregations.extend(field_aggs)
        for case in self.cases:
            case_aggs = case.value.build_reverse_aggregations(resource_spec, resource_id)
#            case_aggs = case_aggs[1:]
            aggregations.extend(case_aggs)
        return aggregations

    def aggregation(self, self_id, user=None):
        comparable_value = self.resource_ref.calculate(self_id)

        for case in self.cases:
            if comparable_value == case.key.value:
                return case.value.aggregation(self_id, user)
        return [], None, False


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
            raise SyntaxError("Both sides of ternary must return same type")

    def infer_type(self):
        return self.then_clause.infer_type()

    def calculate(self, self_id):
        if self.condition.calculate(self_id):
            return self.then_clause.calculate(self_id)
        else:
            return self.else_clause.calculate(self_id)

    def build_reverse_aggregations(self, resource_spec, resource_id):
        cond_aggs = self.condition.build_reverse_aggregations(resource_spec, resource_id)
        then_aggs = self.then_clause.build_reverse_aggregations(resource_spec, resource_id)
        else_aggs = self.else_clause.build_reverse_aggregations(resource_spec, resource_id)
        cond_aggs = cond_aggs[1:] # remove trackers
        then_aggs = then_aggs[1:] # remove trackers
        else_aggs = else_aggs[1:] # remove trackers
        return [[]] + cond_aggs + then_aggs + else_aggs # add dummy tracker ( as calcs are top level )

    def __repr__(self):
        return "%s => %s : %s" % (self.condition, self.then_clause, self.else_clause)


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

    def build_reverse_aggregations(self, resource_spec, resource_id):
        return self.calc.build_reverse_aggregations(resource_spec, resource_id)

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


class FunctionCall(Calc):
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
            spec = self._parser.spec
            cursor = spec.schema.db["resource_%s" % spec.name].aggregate(aggregate_query)
            return cursor.next()['_max']
        except StopIteration:
            return None

    def _min(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_min': {'$min': '$' + spec.name}}})
        # run mongo query from from root_resource collection
        try:
            spec = self._parser.spec
            cursor = spec.schema.db["resource_%s" % spec.name].aggregate(aggregate_query)
            return cursor.next()['_min']
        except StopIteration:
            return None

    def _average(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_average': {'$avg': '$' + spec.name}}})
        # run mongo query from from root_resource collection
        try:
            spec = self._parser.spec
            cursor = spec.schema.db["resource_%s" % spec.name].aggregate(aggregate_query)
            return cursor.next()['_average']
        except StopIteration:
            return None

    def _sum(self, self_id, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(self_id)
        aggregate_query.append({'$group': {'_id': None, '_sum': {'$sum': '$' + aggregate_field.field_name}}})
        # run mongo query from from root_resource collection
        try:
            spec = self._parser.spec
            cursor = spec.schema.db["resource_%s" % spec.name].aggregate(aggregate_query)
            return cursor.next()['_sum']
        except StopIteration:
            return None

    def build_reverse_aggregations(self, resource_spec, resource_id):
        # take into account different functions and params
        return self.params[0].build_reverse_aggregations(resource_spec, resource_id)

    def __repr__(self):
        return "%s(%s)" % (self.func_name, [str(s) for s in self.params])


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
            [(ParameterList, ',', Calc), ParameterList],
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
            [(Operator, '->', ResourceRef, ':', ResourceRef), self._create_ternary],
            [(Operator, '->', ResourceRef, ':', Calc), self._create_ternary],
            [(Operator, '->', Calc, ':', ResourceRef), self._create_ternary],

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
