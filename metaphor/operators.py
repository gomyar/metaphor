


def _calc_average(aggregate):
    total = 0
    values = [res[aggregate.field_name] for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return sum(values) / len(values)
    else:
        return None


def _calc_max(aggregate):
    total = 0
    values = [res[aggregate.field_name] for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return max(values)
    else:
        return None


def _calc_min(aggregate):
    total = 0
    values = [res[aggregate.field_name] for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return min(values)
    else:
        return None


def _calc_sum(aggregate):
    total = 0
    values = [res[aggregate.field_name] for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return sum(values)
    else:
        return None


BUILTIN_FUNCTIONS = {
    'average': _calc_average,
    'max': _calc_max,
    'min': _calc_min,
    'sum': _calc_sum,
}


class Calc(object):
    def __init__(self, exp):
        self.exp = exp

    def calculate(self, resource):
        return self.exp.calculate(resource)

    def all_resource_refs(self):
        return self.exp.all_resource_refs()


class Func(object):
    def __init__(self, func_name, resource_ref):
        self.func_name = func_name
        self.resource_ref = resource_ref

    def calculate(self, resource):
        res = self.resource_ref.create_resource(resource)
        return BUILTIN_FUNCTIONS[self.func_name](res)

    def all_resource_refs(self):
        return self.resource_ref.all_resource_refs()


class ConstRef(Calc):
    ''' Represents a constant basic value
        str, int, float
    '''
    def __init__(self, value):
        self.value = value

    def calculate(self, resource):
        return self.value

    def all_resource_refs(self):
        return set()


class FieldRef(Calc):
    ''' Reference to resource field, self.companies[0].name
    '''
    pass


class ResourceRef(Calc):
    ''' Reference to resource, self.companies[0]
    '''
    def calculate(self, resource):
        return self.create_resource(resource).data

    def create_resource(self, resource):
        # find thing here
        parts = self.exp.split('.')
        root_part = parts.pop(0)

        if root_part != 'self':
            resource = resource.spec.schema.root.build_child(root_part)
        while parts:
            parent = resource
            resource = resource.build_child(parts.pop(0))
            resource._parent = parent
        return resource

    def all_resource_refs(self):
        return set([self.exp])


class AddOp(Calc):
    ''' Addition operator, field + field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) + self.rhs.calculate(resource)

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class SubtractOp(Calc):
    ''' Subtract operator, field - field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) - self.rhs.calculate(resource)

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class MultiplyOp(Calc):
    ''' Multiply operator, field * field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) * self.rhs.calculate(resource)

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class DividebyOp(Calc):
    ''' Divideby operator, field / field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) / self.rhs.calculate(resource)

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())
