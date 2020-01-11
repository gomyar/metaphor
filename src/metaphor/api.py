
import os
from metaphor.lrparse.lrparse import parse_url

class Api(object):
    def __init__(self, schema):
        self.schema = schema

    def get(self, path):
        path = path.strip().strip('/')
        if '/' not in path:
            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]
            root_resources = self.schema.db['resource_%s' % root_field_spec.target_spec_name].aggregate(self.create_field_expansion_aggregations(root_spec))
            return [self.encode_resource(root_spec, r) for r in root_resources]
        tree = parse_url(path, self.schema.root)

        aggregate_query, spec, is_aggregate = tree.aggregation(None)

        aggregate_query.extend(self.create_field_expansion_aggregations(spec))

        # links
        # collections

        # run mongo query from from root_resource collection
        cursor = tree.root_collection().aggregate(aggregate_query)

        results = [row for row in cursor]

        if is_aggregate:
            return [self.encode_resource(spec, row) for row in results]
        elif spec.is_field():
            return results[0][self.field_name] if results else None
        else:
            return self.encode_resource(spec, results[0]) if results else None

    def create_field_expansion_aggregations(self, spec):
        aggregate_query = []
        # apply expansions to fields
        for field_name, field in spec.fields.items():
            if field.field_type == 'link':
                # add check for ' if in "expand" parameter'
                aggregate_query.append(
                    {"$lookup": {
                            "from": "resource_%s" % field.target_spec_name,
                            "localField": field_name,
                            "foreignField": "_id",
                            "as": "_expanded_%s" % field_name,
                    }})
        return aggregate_query

    def encode_resource(self, spec, resource_data):
        self_url = os.path.join(resource_data['_parent_canonical_url'], resource_data['_parent_field_name'], self.schema.encodeid(resource_data['_id']))
        encoded = {
            'id': self.schema.encodeid(resource_data['_id']),
            'self': self_url,
        }
        for field_name, field in spec.fields.items():
            field_value = resource_data.get(field_name)
            if field.field_type == 'link':
                if resource_data['_expanded_%s' % field_name]:
                    expanded_field = resource_data['_expanded_%s' % field_name][0]
                    encoded[field_name] = os.path.join(expanded_field['_parent_canonical_url'], expanded_field['_parent_field_name'], self.schema.encodeid(expanded_field['_id']))
                else:
                    encoded[field_name] = None
            elif field.field_type == 'parent_collection':
                if field_value:
                    encoded[field_name] = self.schema.load_canonical_url_for(field.target_spec_name, field_name, self.schema.encodeid(field_value))
                else:
                    encoded[field_name] = None
            elif field.field_type == 'reverse_link':
                encoded[field_name] = "%s/%s" % (self_url, field_name)
            else:
                encoded[field_name] = field_value
        return encoded


