

class ReverseAggregator(object):
    def __init__(self, schema):
        self.schema = schema

    def get_for_resource(self, calc_tree, spec_name, resource_id=None, calc_spec_name=None,
            calc_field_name=None):
        spec = self.schema.specs[spec_name]
        aggregations = calc_tree.build_reverse_aggregations(
            spec,
            self.schema.encodeid(resource_id) if resource_id else None,
            calc_spec_name,
            calc_field_name)
        aggregations.pop(0)
        return aggregations
