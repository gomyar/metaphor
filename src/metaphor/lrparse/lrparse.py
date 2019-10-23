
import tokenize
from StringIO import StringIO

from metaphor.resource import Field
from metaphor.resource import LinkResource
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import ReverseLinkSpec
from metaphor.resource import CollectionSpec
from metaphor.resource import FieldSpec
from metaphor.resource import CalcSpec


class Calc(object):
    def __init__(self, tokens):
        self.tokens = tokens

    def __repr__(self):
        return "C[%s]" % (self.tokens,)

    def calculate(self, resource):
        raise NotImplemented()


class ResourceRef(object):
    def __init__(self, tokens):
        ''' takes resource_ref.name '''
        if isinstance(tokens[0], basestring):
            self.resource_ref = RootResourceRef(tokens[0])
        else:
            self.resource_ref = tokens[0]
        self.field_name = tokens[2]

    def root_collection(self, resource):
        return self.resource_ref.root_collection(resource)

    def root_spec(self, resource):
        return self.resource_ref.root_spec(resource)

    def result_type(self, starting_spec):
        child_resource = self.resource_ref.result_type(starting_spec)
        return child_resource.create_child_spec(self.field_name)

    def calculate(self, resource):
        # do this
        # aggregate subresource
        aggregate_query, spec, is_aggregate = self.aggregation(resource)
        # run mongo query from from root_resource collection
        cursor = self.root_collection(resource).aggregate(aggregate_query)
        # build child resource from data result with result_type() spec
        if type(spec) == FieldSpec:
            if is_aggregate:
                child_resources = []
                for data in cursor:
                    value = data.get(self.field_name)
                    child_resources.append(self.result_type(resource.spec).build_resource(None, self.field_name, value))
                return child_resources
            else:
                value = cursor.next()[self.field_name]
                child_resource = self.result_type(resource.spec).build_resource(None, self.field_name, value)
                return child_resource
        elif type(spec) == CalcSpec:
            return resource.data[spec.field_name]  # unsure what's going on here, bad separation probably
        elif type(spec) in (ResourceLinkSpec, ReverseLinkSpec):
            if is_aggregate:
                child_resources = []
                for data in cursor:
                    child_resources.append(spec.target_spec.build_resource(None, self.field_name, data))
                return child_resources
            else:
                try:
                    data = cursor.next()
                    return spec.target_spec.build_resource(None, self.field_name, data)
                except StopIteration:
                    return None
        else:
            raise Exception("Cannot calculate spec %s" % (spec,))

    def aggregation(self, resource):
        aggregations = []
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(resource)
        aggregations.extend(aggregation)
        child_spec = spec.create_child_spec(self.field_name)
        if isinstance(child_spec, ReverseLinkSpec):
            # if link
            aggregation.append(
                {"$lookup": {
                        "from": "resource_%s" % (child_spec.name,),
                        "localField": "_id",
                        "foreignField": child_spec.target_field_name,
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
            is_aggregate = True
        elif isinstance(child_spec, ResourceLinkSpec):
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
        elif isinstance(child_spec, CollectionSpec):
            # if linkcollection / collection
            aggregation.append(
                {"$lookup": {
                        "from": "resource_%s" % (child_spec.name,),
                        "localField": "_id",
                        "foreignField": "_owners.owner_id",
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
            is_aggregate = True
        elif isinstance(child_spec, FieldSpec):
            aggregation.append(
                {"$project": {
                    child_spec.field_name: True,
                }})
        elif isinstance(child_spec, CalcSpec):
            pass
        else:
            raise Exception("Unrecognised spec %s" % (child_spec,))
        return aggregation, child_spec, is_aggregate

    def __repr__(self):
        return "R[%s %s]" % (self.resource_ref, self.field_name)

    def all_resource_refs(self):
        return set([self.resource_ref_snippet()])

    def resource_ref_snippet(self):
        return self.resource_ref.resource_ref_snippet() + '.' + self.field_name


class FieldRef(ResourceRef, Calc):
    pass


class RootResourceRef(ResourceRef):
    def __init__(self, tokens):
        ''' takes single name for root resource'''
        self.resource_name = tokens

    def calculate(self, resource):
        # resolve resource
        if self.resource_name == 'self':
            return resource
        else:
            return resource.spec.schema.root.build_child(self.resource_name)

    def result_type(self, starting_spec):
        if self.resource_name == 'self':
            return starting_spec
        else:
            return starting_spec.schema.root.build_child(self.resource_name).spec

    def root_collection(self, resource):
        if self.resource_name == 'self':
            return resource.spec._collection()
        else:
            return self.root_spec(resource)._collection()

    def root_spec(self, resource):
        return resource.spec.schema.root.build_child(self.resource_name).spec

    def aggregation(self, resource):
        if self.resource_name == 'self':
            aggregation = [
                {"$match": {"_id": resource._id}}
            ]
            spec = resource.spec
            return aggregation, spec, False
        else:
            aggregation = []
            spec = self.root_spec(resource)
            return aggregation, spec, True

    def __repr__(self):
        return "T[%s]" % (self.resource_name,)

    def all_resource_refs(self):
        return set()

    def resource_ref_snippet(self):
        return self.resource_name


class FilteredResourceRef(ResourceRef):
    def __init__(self, tokens):
        if type(tokens[0]) is str:
            self.resource_ref = RootResourceRef(tokens[0])
        else:
            self.resource_ref = tokens[0]
        self.filter_ref = tokens[1]

    def result_type(self, starting_spec):
        return self.resource_ref.result_type(starting_spec)

    def aggregation(self, resource):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(resource)
        filter_agg = self.filter_ref.filter_aggregation(spec)
        aggregation.append(filter_agg)
        return aggregation, spec, True

    def calculate(self, resource):
        # will need aggregate?
        return self.resource_ref.calculate(resource)

    def resource_ref_snippet(self):
        return self.resource_ref.resource_ref_snippet()

    def __repr__(self):
        return "F[%s %s]" % (self.resource_ref, self.filter_ref)


class ConstRef(Calc):
    ALLOWED_TYPES = (int, float, str, unicode)

    def __init__(self, tokens):
        const = tokens[0]
        if const[0] == '"' and const[-1] == '"':
            self.value = const.strip('"')
        elif const[0] == "'" and const[-1] == "'":
            self.value = const.strip("'")
        else:
            try:
                self.value = int(const)
            except ValueError as v:
                self.value = float(const)

    def calculate(self, resource):
        return self.value

    def __repr__(self):
        return "C[%s]" % (self.value,)

    def all_resource_refs(self):
        return set()


class Operator(Calc):
    def __init__(self, tokens):
        self.lhs = tokens[0]
        self.op = tokens[1]
        self.rhs = tokens[2]

    def calculate(self, resource):
        lhs = self.lhs.calculate(resource)
        rhs = self.rhs.calculate(resource)
        try:
            if type(lhs) not in ConstRef.ALLOWED_TYPES:
                lhs = lhs.data
            if type(rhs) not in ConstRef.ALLOWED_TYPES:
                rhs = rhs.data
            op = self.op
            if op == '+':
                return (lhs or 0) + (rhs or 0)
            elif op == '-':
                return (lhs or 0) - (rhs or 0)
            elif op == '*':
                return lhs * rhs
            elif op == '/':
                return lhs / rhs
        except TypeError as te:
            return None
        except AttributeError as te:
            return None

    def __repr__(self):
        return "O[%s%s%s]" % (self.lhs, self.op, self.rhs)

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class Condition(object):
    OPERATORS = {
        '=': '$eq',
        '>': '$gt',
        '<': '$lt',
    }

    def __init__(self, tokens):
        self.field_name, self.operator, self.const = tokens

    def condition_aggregation(self, spec):
        field_spec = spec.create_child_spec(self.field_name)
        if not field_spec.check_comparable_type(self.const.value):
            raise Exception("Incorrect type for condition: %s %s %s" %(
                self.field_name, self.operator, self.const.value))
        aggregation = {
            self.field_name: {
                self.OPERATORS[self.operator]: self.const.value}}
        return aggregation

    def __repr__(self):
        return "O[%s %s %s]" % (self.field_name, self.operator, self.const)


class Filter(object):
    def __init__(self, tokens):
        self.condition = tokens[1]

    def filter_aggregation(self, spec):
        agg = self.condition.condition_aggregation(spec)
        aggregation = {"$match": agg}
        return aggregation

class ParameterList(object):
    def __init__(self, tokens):
        if type(tokens[0]) is ParameterList:
            self.params = tokens[0].params
            self.params.append(tokens[2])
        else:
            self.params = [tokens[0], tokens[2]]

    def __repr__(self):
        return "  %s  " % (self.params,)

    def all_resource_refs(self):
        refs = set()
        for param in self.params:
            refs = refs.union(param.all_resource_refs())
        return refs


class Brackets(Calc):
    def __init__(self, tokens):
        self.calc = tokens[1]

    def calculate(self, resource):
        return self.calc.calculate(resource)

    def all_resource_refs(self):
        return self.calc.all_resource_refs()

    def __repr__(self):
        return "(" + str(self.calc) + ")"


class FunctionCall(Calc):
    def __init__(self, tokens):
        self.func_name = tokens[0]

        if isinstance(tokens[1], Brackets):
            self.params = [tokens[1].calc]
        elif len(tokens) == 1 and isinstance(tokens[0], basestring):
            # case when simple NAME
            self.params = [RootResourceRef(tokens[0])]
        elif len(tokens) == 4 and isinstance(tokens[2], basestring):
            # case when simple NAME
            self.params = [RootResourceRef(tokens[2])]
        else:
            self.params = [p for p in tokens[2].params]

        self.functions = {
            'round': self._round,
            'max': self._max,
            'min': self._min,
            'average': self._average,
            'sum': self._sum,
        }

    def calculate(self, resource):
        return self.functions[self.func_name](resource, *self.params)

    def all_resource_refs(self):
        refs = set()
        for param in self.params:
            refs = refs.union(param.all_resource_refs())
        return refs

    def _round(self, resource, value, digits=None):
        value = value.calculate(resource)

        if isinstance(value, Field):
            value = value.data
        elif isinstance(value, ConstRef):
            value = value.value

        if digits:
            return round(value, digits.calculate(resource))
        else:
            return round(value)

    def _max(self, resource, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(resource)
        aggregate_query.append({'$group': {'_id': None, '_max': {'$max': '$' + spec.field_name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection(resource).aggregate(aggregate_query)
            return cursor.next()['_max']
        except StopIteration:
            return None

    def _min(self, resource, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(resource)
        aggregate_query.append({'$group': {'_id': None, '_min': {'$min': '$' + spec.field_name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection(resource).aggregate(aggregate_query)
            return cursor.next()['_min']
        except StopIteration:
            return None

    def _average(self, resource, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(resource)
        aggregate_query.append({'$group': {'_id': None, '_average': {'$avg': '$' + spec.field_name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection(resource).aggregate(aggregate_query)
            return cursor.next()['_average']
        except StopIteration:
            return None

    def _sum(self, resource, aggregate_field):
        aggregate_query, spec, is_aggregate = aggregate_field.aggregation(resource)
        aggregate_query.append({'$group': {'_id': None, '_sum': {'$sum': '$' + spec.field_name}}})
        # run mongo query from from root_resource collection
        try:
            cursor = aggregate_field.root_collection(resource).aggregate(aggregate_query)
            return cursor.next()['_sum']
        except StopIteration:
            return None

    def __repr__(self):
        return "%s(%s)" % (self.func_name, [str(s) for s in self.params])


NAME = 'NAME'
STRING = 'STRING'
NUMBER = 'NUMBER'

def lex(raw_tokens):
    ignore_list = [
        tokenize.NEWLINE,
        tokenize.COMMENT,
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
            tokens.append((NAME, value))
        elif value in ignore_list:
            pass
        elif token_type == tokenize.ENDMARKER:
            pass
        elif token_type == tokenize.NEWLINE:
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
            [(Calc, '+', Calc) , Operator],
            [(Calc, '-', Calc) , Operator],
            [(Calc, '*', Calc) , Operator],
            [(Calc, '/', Calc) , Operator],
            [(Condition, '&', Condition) , Condition],
            [('[', Condition, ']'), Filter],
            [(Calc, ',', Calc), ParameterList],
            [(ParameterList, ',', Calc), ParameterList],
            [('(', Calc, ')'), Brackets],
            [(NAME, Brackets), FunctionCall],
            [(NAME, '(', ParameterList, ')'), FunctionCall],
            [(NAME, '(', NAME, ')'), FunctionCall],
            [(STRING,), ConstRef],
            [(NUMBER,), ConstRef],
            [(ResourceRef, Filter), FilteredResourceRef],
            [(NAME, Filter), FilteredResourceRef],
            [(ResourceRef, '.', NAME) , self._create_resource_ref],
            [(NAME, '.', NAME) , self._create_resource_ref],
        ]

    def _create_resource_ref(self, tokens):
        resource_ref = ResourceRef(tokens)
        if type(resource_ref.result_type(self.spec)) in (FieldSpec, CalcSpec):
            return FieldRef(tokens)
        else:
            return resource_ref

    def match_pattern(self, pattern, last_tokens):
        def m(pat, tok):
            return pat == tok[0] or (not isinstance(tok[0], basestring) and not isinstance(pat, basestring) and issubclass(tok[0], pat))
        return len(pattern) == len(last_tokens) and all(m(pat, tok) for pat, tok in zip(pattern, last_tokens))

    def match_and_reduce(self):
        for pattern, reduced_class in self.patterns:
            last_shifted = self.shifted[-len(pattern):]
#            print "Checking %s against %s" % (pattern,last_shifted)
            if self.match_pattern(pattern, last_shifted):
#                print "** Reducing %s(%s)" % (reduced_class, last_shifted)
                self._reduce(reduced_class, last_shifted)
#                print "** shifted: %s" % (self.shifted,)
                return True

        return False

    def parse(self):
        while self.tokens:
            self.shifted.append(self.tokens.pop(0))
#            print "Shifted: %s" % (self.shifted,)
            while self.match_and_reduce():
                pass

        if len(self.shifted) > 1:
#            print "Errors left: Shifted: %s" % (self.shifted,)
            raise Exception("Unexpected '%s'" % (self.shifted[1],))

        if self.shifted[0][1] == 'self':
            raise Exception("Calc cannot be 'self' only.")
        return self.shifted[0][1]

    def _reduce(self, reduced_class, tokens):
        self.shifted = self.shifted[:-len(tokens)]
        reduction = reduced_class([a[1] for a in tokens])
        self.shifted.append((type(reduction), reduction))


def parse(line, spec=None):
    tokens = tokenize.generate_tokens(StringIO(line).next)
    return Parser(lex(tokens), spec).parse()
