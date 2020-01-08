
import tokenize
from io import StringIO


class Calc(object):
    def __init__(self, tokens, parser):
        self.tokens = tokens
        self._parser = parser

    def __repr__(self):
        return "C[%s]" % (self.tokens,)

    def calculate(self, self_id):
        raise NotImplemented()


class ResourceRef(object):
    def __init__(self, tokens, parser):
        self._parser = parser
        ''' takes resource_ref.name '''
        if isinstance(tokens[0], str):
            self.resource_ref = RootResourceRef(tokens[0], self._parser)
        else:
            self.resource_ref = tokens[0]
        self.field_name = tokens[2]

    def root_collection(self):
        return self._parser.spec.schema.db['resource_%s' % self._parser.spec.name]

    def infer_type(self):
        return self.resource_ref.infer_type().build_child_spec(self.field_name)

    def is_collection(self):
        return self.aggregation('ID000000000000000000000000')[2]

    def calculate(self, self_id):
        # do this
        # aggregate subresource
        aggregate_query, spec, is_aggregate = self.aggregation(self_id)
        # run mongo query from from root_resource collection
        cursor = self.root_collection().aggregate(aggregate_query)

        results = [row for row in cursor]
        if is_aggregate:
            return [self._parser.spec.schema.encode_resource(spec, row) for row in results]
        elif spec.is_field():
            return results[0][self.field_name] if results else None
        else:
            return self._parser.spec.schema.encode_resource(results[0]) if results else None

    def aggregation(self, self_id):
        aggregations = []
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id)
        aggregations.extend(aggregation)
        child_spec = spec.build_child_spec(self.field_name)
        child_type = spec.fields[self.field_name].field_type
        if child_type == 'reverse_link':
            aggregation.append(
                {"$lookup": {
                        "from": "resource_%s" % (child_spec.name,),
                        "localField": "_id",
                        "foreignField": spec.name,
                        "as": "_field_%s" % (self.field_name,),
                }})
            is_aggregate = True
            aggregation.append(
                {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
            )
            aggregation.append(
                {"$unwind": "$_id"}
            )
            aggregation.append(
                {"$replaceRoot": {"newRoot": "$_id"}}
            )

        elif child_type == 'reverse_link_collection':
            # when a reverse aggregate is followed by another reverse aggregate
            # reverse link to collection (through _owners)
            aggregation.append(
                {"$lookup": {
                        "from": "resource_%s" % (child_spec.name,),
                        "localField": "_owners.owner_id",
                        "foreignField": "_id",
                        "as": "_field_%s" % (self.field_name,),
                }})
            is_aggregate = True
            aggregation.append(
                {'$group': {'_id': '$_field_%s' % (self.field_name,)}}
            )
            aggregation.append(
                {"$unwind": "$_id"}
            )
            aggregation.append(
                {"$replaceRoot": {"newRoot": "$_id"}}
            )
        elif child_type == 'link':
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
        elif child_type == 'collection':
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
        elif spec.fields[self.field_name].is_primitive():
            aggregation.append(
                {"$project": {
                    self.field_name: True,
                }})
        elif child_type == 'calc':
            # check type:
            if spec.fields[self.field_name].is_primitive():
                # int float str
                pass
            else:
                # resource
                # lookup
                aggregation.append(
                    {"$lookup": {
                            "from": "resource_%s" % (child_spec.calc_type,),
                            "localField": child_spec.name,
                            "foreignField": "_id",
                            "as": "_field_%s" % (child_spec.name,),
                    }})
                aggregation.append(
                    {'$group': {'_id': '$_field_%s' % (child_spec.name,)}}
                )
                aggregation.append(
                    {"$unwind": "$_id"}
                )
                aggregation.append(
                    {"$replaceRoot": {"newRoot": "$_id"}}
                )
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
    def __init__(self, tokens, parser):
        ''' takes single name for root resource'''
        self.resource_name = tokens
        self._parser = parser

    def calculate(self, self_id):
        raise NotImplemented("calculate not implements for RootResourceRef")

    def infer_type(self):
        return self._parser.spec

    def aggregation(self, self_id):
        if self.resource_name == 'self':
            aggregation = [
                {"$match": {"_id": self._parser.spec.schema.decodeid(self_id)}}
            ]
            return aggregation, self._parser.spec, False
        else:
            return [], self._parser.spec, True

    def __repr__(self):
        return "T[%s]" % (self.resource_name,)

    def all_resource_refs(self):
        return set()

    def resource_ref_snippet(self):
        return self.resource_name


class FilteredResourceRef(ResourceRef):
    def __init__(self, tokens, parser):
        self._parser = parser
        if type(tokens[0]) is str:
            self.resource_ref = RootResourceRef(tokens[0], self._parser)
        else:
            self.resource_ref = tokens[0]
        self.filter_ref = tokens[1]

    def infer_type(self):
        return self.resource_ref.infer_type()

    def aggregation(self, self_id):
        aggregation, spec, is_aggregate = self.resource_ref.aggregation(self_id)
        filter_agg = self.filter_ref.filter_aggregation(spec)
        aggregation.append(filter_agg)
        return aggregation, spec, True

#    def calculate(self, resource):
#        # will need aggregate?
#        return self.resource_ref.calculate(resource)

    def resource_ref_snippet(self):
        return self.resource_ref.resource_ref_snippet()

    def __repr__(self):
        return "F[%s %s]" % (self.resource_ref, self.filter_ref)


class ConstRef(Calc):
    ALLOWED_TYPES = (int, float, str)

    def __init__(self, tokens, parser):
        self._parser = parser
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

    def calculate(self, self_id):
        return self.value

    def __repr__(self):
        return "C[%s]" % (self.value,)

    def all_resource_refs(self):
        return set()


class Operator(Calc):
    def __init__(self, tokens, parser):
        self.lhs = tokens[0]
        self.op = tokens[1]
        self.rhs = tokens[2]
        self._parser = parser

    def calculate(self, self_id):
        lhs = self.lhs.calculate(self_id)
        rhs = self.rhs.calculate(self_id)
        try:
#            if type(lhs) not in ConstRef.ALLOWED_TYPES:
#                lhs = lhs.data
#            if type(rhs) not in ConstRef.ALLOWED_TYPES:
#                rhs = rhs.data
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

    def __init__(self, tokens, parser):
        self.field_name, self.operator, self.const = tokens
        self._parser = parser

    def condition_aggregation(self, spec):
        field_spec = spec.build_child_spec(self.field_name)
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
    def __init__(self, tokens, parser):
        self.condition = tokens[1]
        self._parser = parser

    def __repr__(self):
        return "[%s]" % (self.condition,)

    def filter_aggregation(self, spec):
        agg = self.condition.condition_aggregation(spec)
        aggregation = {"$match": agg}
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

    def all_resource_refs(self):
        refs = set()
        for param in self.params:
            refs = refs.union(param.all_resource_refs())
        return refs


class Brackets(Calc):
    def __init__(self, tokens, parser):
        self.calc = tokens[1]
        self._parser = parser

    def calculate(self, self_id):
        return self.calc.calculate(self_id)

    def all_resource_refs(self):
        return self.calc.all_resource_refs()

    def __repr__(self):
        return "(" + str(self.calc) + ")"


class FunctionCall(Calc):
    def __init__(self, tokens, parser):
        self.func_name = tokens[0]
        self._parser = parser

        if isinstance(tokens[1], Brackets):
            self.params = [tokens[1].calc]
        elif len(tokens) == 1 and isinstance(tokens[0], str):
            # case when simple NAME
            self.params = [RootResourceRef(tokens[0], self._parser)]
        elif len(tokens) == 4 and isinstance(tokens[2], str):
            # case when simple NAME
            self.params = [RootResourceRef(tokens[2], self._parser)]
        else:
            self.params = [p for p in tokens[2].params]

        self.functions = {
            'round': self._round,
            'max': self._max,
            'min': self._min,
            'average': self._average,
            'sum': self._sum,
        }

    def calculate(self, self_id):
        return self.functions[self.func_name](self_id, *self.params)

    def all_resource_refs(self):
        refs = set()
        for param in self.params:
            refs = refs.union(param.all_resource_refs())
        return refs

    def _round(self, self_id, aggregate_field, digits=None):
        value = aggregate_field.calculate(self_id)

#        if isinstance(value, Field):
#            value = value.data
#        elif isinstance(value, ConstRef):
#            value = value.value

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
        aggregate_query.append({'$group': {'_id': None, '_sum': {'$sum': '$' + spec.name}}})
        # run mongo query from from root_resource collection
        try:
            spec = self._parser.spec
            cursor = spec.schema.db["resource_%s" % spec.name].aggregate(aggregate_query)
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
            [(ResourceRef, '.', NAME), self._create_resource_ref],
            [(NAME, '.', NAME), self._create_resource_ref],
        ]

    def _create_resource_ref(self, tokens, parser):
        resource_ref = ResourceRef(tokens, parser)
        if resource_ref.infer_type().is_field():
            return FieldRef(tokens, parser)
        else:
            return resource_ref

    def match_pattern(self, pattern, last_tokens):
        def m(pat, tok):
            return pat == tok[0] or (not isinstance(tok[0], str) and not isinstance(pat, str) and issubclass(tok[0], pat))
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
        reduction = reduced_class([a[1] for a in tokens], self)
        self.shifted.append((type(reduction), reduction))


def parse(line, spec):
    tokens = tokenize.generate_tokens(StringIO(line).read)
    return Parser(lex(tokens), spec).parse()
