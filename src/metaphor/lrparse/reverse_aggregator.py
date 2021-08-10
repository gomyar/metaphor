

class ReverseAggregator(object):
    def __init__(self, schema):
        self.schema = schema

    def get_for_resource(self, calc_tree, spec_name, resource_id):
        spec = self.schema.specs[spec_name]
        aggregations = calc_tree.build_reverse_aggregations(spec, self.schema.encodeid(resource_id))
        aggregations.pop(0)
        return aggregations
