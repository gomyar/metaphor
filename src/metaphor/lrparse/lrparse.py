
import tokenize
from StringIO import StringIO

from metaphor.resource import Field
from metaphor.resource import LinkResource
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import ReverseLinkSpec
from metaphor.resource import CollectionSpec
from metaphor.resource import FieldSpec


class Calc(object):
    def __init__(self, tokens):
        self.tokens = tokens

    def __repr__(self):
        return "C[%s]" % (self.tokens,)

    def calculate(self, resource):
        raise NotImplemented()


class ResourceRef(Calc):
    def __init__(self, tokens):
        ''' takes resource_ref.name '''
        if type(tokens[0]) is str:
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
        elif type(spec) in (ResourceLinkSpec, ReverseLinkSpec):
            if is_aggregate:
                child_resources = []
                for data in cursor:
                    child_resources.append(spec.target_spec.build_resource(None, self.field_name, data))
                return child_resources
            else:
                data = cursor.next()
                return spec.target_spec.build_resource(None, self.field_name, data)
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
                        "foreignField": spec.field_name,
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
                {'$group': {'_uniq': '$_field_%s' % (self.field_name,)}}
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
        else:
            raise Exception("Unrecognised spec %s" % (child_spec,))
        return aggregation, child_spec, is_aggregate

    def __repr__(self):
        return "R[%s %s]" % (self.resource_ref, self.field_name)

    def all_resource_refs(self):
        return [self.resource_ref_snippet()]

    def resource_ref_snippet(self):
        return self.resource_ref.resource_ref_snippet() + '.' + self.field_name


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
        return []

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
        return float(self.value)

    def __repr__(self):
        return "C[%s]" % (self.value,)


class OperatorResult(object):
    def __init__(self, value):
        self.data = value


class Operator(Calc):
    def __init__(self, tokens):
        self.tokens = tokens

    def calculate(self, resource):
        lhs = self.tokens[0].calculate(resource)
        rhs = self.tokens[2].calculate(resource)
        op = self.tokens[1]
        if op == '+':
            return OperatorResult(lhs.data + rhs.data)
        elif op == '-':
            return OperatorResult(lhs.data - rhs.data)
        elif op == '*':
            return OperatorResult(lhs.data * rhs.data)
        elif op == '/':
            return OperatorResult(lhs.data / rhs.data)

    def __repr__(self):
        return "O[%s]" % (self.tokens,)


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
        return "f(%s)" % (self.params,)


class FunctionCall(Calc):
    def __init__(self, tokens):
        self.tokens = tokens
        self.functions = {
            'round': self._round,
        }

    def calculate(self, resource):
        return self.functions[self.tokens[0]](resource, *self.tokens[1:])

    def _round(self, resource, value, digits=None):
        return round(value.calculate(resource), digits)

    def __repr__(self):
        return "f(%s)" % (self.tokens,)


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
        else:
            raise Exception("Unexpected token %s at line %s col %s" % (
                            value, line, col))
    return tokens


class Parser(object):

    def __init__(self, tokens):
        self.tokens = tokens
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
            [(NAME, '(', Calc, ')'), FunctionCall],
            [(NAME, '(', ParameterList, ')'), FunctionCall],
            [(STRING,), ConstRef],
            [(NUMBER,), ConstRef],
            [(ResourceRef, Filter), FilteredResourceRef],
            [(NAME, Filter), FilteredResourceRef],
            [(ResourceRef, '.', NAME) , ResourceRef],
            [(NAME, '.', NAME) , ResourceRef],
        ]

    def match_pattern(self, pattern, last_tokens):
        def m(pat, tok):
            return pat == tok[0] or (type(tok[0]) != str and type(pat) != str and issubclass(tok[0], pat))
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
        return self.shifted[0][1]

    def _reduce(self, reduced_class, tokens):
        self.shifted = self.shifted[:-len(tokens)]
        self.shifted.append((reduced_class, reduced_class([a[1] for a in tokens])))


def parse(line):
    tokens = tokenize.generate_tokens(StringIO(line).readline)
    return  Parser(lex(tokens)).parse()
