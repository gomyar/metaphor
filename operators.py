

class Calc(object):
    def __init__(self, exp):
        self.exp = exp

    def calculate(self, resource):
        return self.exp.calculate(resource)


class ConstRef(Calc):
    ''' Represents a constant basic value
        str, int, float
    '''
    def __init__(self, value):
        self.value = value

    def calculate(self, resource):
        return self.value


class FieldRef(Calc):
    ''' Reference to resource field, self.companies[0].name
    '''
    pass


class ResourceRef(Calc):
    ''' Reference to resource, self.companies[0]
    '''
    def calculate(self, resource):
        # find thing here
        parts = self.exp.split('.')
        if parts.pop(0) != 'self':
            resource = resource.spec.schema.root
        while parts:
            parent = resource
            resource = resource.build_child(parts.pop(0))
            resource._parent = parent

        return resource.data


class FunctionRef(Calc):
    ''' Reference to built-in or extention function,
        my_func(field1, field2)
    '''
    pass


class AddOp(Calc):
    ''' Addition operator, field + field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) + self.rhs.calculate(resource)


class SubtractOp(Calc):
    ''' Subtract operator, field - field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) - self.rhs.calculate(resource)


class MultiplyOp(Calc):
    ''' Multiply operator, field * field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) * self.rhs.calculate(resource)


class DividebyOp(Calc):
    ''' Divideby operator, field / field
    '''
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def calculate(self, resource):
        return self.lhs.calculate(resource) / self.rhs.calculate(resource)
