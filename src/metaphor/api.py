
from metaphor.lrparse.lrparse import parse_url

class Api(object):
    def __init__(self, schema):
        self.schema = schema

    def get(self, path):
        path = path.strip().strip('/')
        if '/' not in path:
            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]
            root_resources = self.schema.db['resource_%s' % root_field_spec.target_spec_name].find()
            return [self.schema.encode_resource(root_spec, r) for r in root_resources]
        tree = parse_url(path, self.schema.root)

        aggregate_query, spec, is_aggregate = tree.aggregation(None)
        # run mongo query from from root_resource collection
        cursor = tree.root_collection().aggregate(aggregate_query)

        results = [row for row in cursor]

        if is_aggregate:
            return [self.schema.encode_resource(spec, row) for row in results]
        elif spec.is_field():
            return results[0][self.field_name] if results else None
        else:
            return self.schema.encode_resource(spec, results[0]) if results else None
