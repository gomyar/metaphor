


def _filter_one(aggregate, field_name, field_value):
    for res in aggregate.iterate_aggregate_cursor():
        if res.build_child(field_name).data == field_value:
            return res
    return None


def _filter_max(aggregate, field_name):
    return max(aggregate.iterate_aggregate_cursor(), key=lambda val: val.build_child(field_name).data)


def _filter_min(aggregate, field_name):
    return min(aggregate.iterate_aggregate_cursor(), key=lambda val: val.build_child(field_name).data)


def _calc_average(aggregate):
    total = 0
    values = [res.get(aggregate.field_name) for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return sum(values) / len(values)
    else:
        return None


def _calc_max(aggregate):
    total = 0
    values = [res.get(aggregate.field_name) for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return max(values)
    else:
        return None


def _calc_min(aggregate):
    total = 0
    values = [res.get(aggregate.field_name) for res in aggregate.serialize('')]
    values = [v for v in values if v is not None]
    if values:
        return min(values)
    else:
        return None


def _calc_sum(aggregate):
    total = 0
    values = [res.get(aggregate.field_name) for res in aggregate.serialize('')]
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
        try:
            res = self.resource_ref.create_resource(resource)
            if self.func_name in BUILTIN_FUNCTIONS:
                return BUILTIN_FUNCTIONS[self.func_name](res)
            else:
                return res.spec.schema.execute_function(self.func_name, res)
        except TypeError as te:
            return float('nan')

    def all_resource_refs(self):
        return self.resource_ref.all_resource_refs()


class FilterOneFunc(object):
    def __init__(self, resource_ref, field_name_ref, field_value_ref):
        self.resource_ref = resource_ref
        self.field_name = field_name_ref.value
        self.field_value = field_value_ref.value

    def calculate(self, resource):
        res = self.resource_ref.create_resource(resource)
        return _filter_one(res, self.field_name, self.field_value)


class FilterMaxFunc(object):
    def __init__(self, resource_ref, field_name_ref):
        self.resource_ref = resource_ref
        self.field_name = field_name_ref.value

    def calculate(self, resource):
        res = self.resource_ref.create_resource(resource)
        return _filter_max(res, self.field_name)


class FilterMinFunc(object):
    def __init__(self, resource_ref, field_name_ref):
        self.resource_ref = resource_ref
        self.field_name = field_name_ref.value

    def calculate(self, resource):
        res = self.resource_ref.create_resource(resource)
        return _filter_min(res, self.field_name)


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
    def calculate(self, parent):
        resource = self.create_resource(parent)
        return resource._id if resource._id else resource.data

    def create_resource(self, resource):
        parts = self.exp.split('.')
        root_part = parts.pop(0)

        if root_part != 'self':
            resource = resource.spec.schema.root.build_child(root_part)
        while parts:
            resource = resource.build_child(parts.pop(0))
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
        lhs_val = self.lhs.calculate(resource)
        rhs_val = self.rhs.calculate(resource)
        if lhs_val == None and rhs_val == None:
            return None
        if lhs_val == None:
            return rhs_val
        if rhs_val == None:
            return lhs_val
        return lhs_val + rhs_val

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class SubtractOp(Calc):
    ''' Subtract operator, field - field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        lhs_val = self.lhs.calculate(resource)
        rhs_val = self.rhs.calculate(resource)
        if lhs_val == None and rhs_val == None:
            return None
        if lhs_val == None:
            return -rhs_val
        if rhs_val == None:
            return lhs_val
        return lhs_val - rhs_val

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class MultiplyOp(Calc):
    ''' Multiply operator, field * field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        lhs_val = self.lhs.calculate(resource)
        rhs_val = self.rhs.calculate(resource)
        if lhs_val == None or rhs_val == None:
            return None
        return lhs_val * rhs_val

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())


class DividebyOp(Calc):
    ''' Divideby operator, field / field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        lhs_val = self.lhs.calculate(resource)
        rhs_val = self.rhs.calculate(resource)
        if lhs_val == None or rhs_val == None or rhs_val == 0:
            return None
        return lhs_val / rhs_val

    def all_resource_refs(self):
        return self.lhs.all_resource_refs().union(self.rhs.all_resource_refs())
