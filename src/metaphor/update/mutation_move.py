
from metaphor.lrparse.lrparse import parse_canonical_url


class MoveMutation:
    def __init__(self, mutation, schema, from_path, to_path):
        self.mutation = mutation
        self.schema = schema

        self.from_path = from_path
        self.to_path = to_path

    def __repr__(self):
        return "<MoveMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        if '/' in self.to_path:
            parent_path, field_name = self.to_path.rsplit('/', 1)
            tree = parse_canonical_url(parent_path, self.schema.root)  # "schema" or "schema" depending?

            aggregate_query = tree.create_aggregation()
            spec = tree.infer_type()

            field_spec = spec.fields[field_name]

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            return self.mutation.updater.move_resource(self.from_path, self.to_path, parent_resource['_id'], field_name, spec.name)
        else:
            field_name = self.to_path
            root_field_spec = self.schema.root.fields[self.to_path]
            return self.mutation.updater.move_resource_to_root(self.from_path, self.to_path, root_field_spec.target_spec_name)

        # if filtered non-root resources
        #   for each resource in filter
        #   nested agg using filter
        #   alter parent info


