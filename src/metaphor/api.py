
import os
from metaphor.lrparse.lrparse import parse
from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url
from metaphor.lrparse.lrparse import parse_filter
from metaphor.lrparse.lrparse import CollectionResourceRef
from metaphor.lrparse.lrparse import RootResourceRef
from metaphor.lrparse.lrparse import LinkCollectionResourceRef
from metaphor.schema import CalcField
from metaphor.updater import Updater
from bson.errors import InvalidId


class Api(object):
    def __init__(self, schema):
        self.schema = schema
        self.updater = Updater(schema)

    def patch(self, path, data):
        path = path.strip().strip('/')
        tree = parse_canonical_url(path, self.schema.root)
        aggregate_query, spec, is_aggregate = tree.aggregation(None)

        cursor = tree.root_collection().aggregate(aggregate_query)

        resource = next(cursor)

        return self.updater.update_fields(
            spec.name,
            self.schema.encodeid(resource['_id']),
            data)

    def post(self, path, data):
        path = path.strip().strip('/')

        if '/' in path:
            parent_path, field_name = path.rsplit('/', 1)
            tree = parse_canonical_url(parent_path, self.schema.root)
            aggregate_query, spec, is_aggregate = tree.aggregation(None)

            field_spec = spec.fields[field_name]

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)
            parent_id = self.schema.encodeid(parent_resource['_id'])

            if field_spec.field_type == 'linkcollection':
                return self.updater.create_linkcollection_entry(
                    spec.name,
                    parent_id,
                    field_name,
                    data['id'])
            else:
                return self.updater.create_resource(
                    field_spec.target_spec_name,
                    spec.name,
                    field_name,
                    parent_id,
                    data)
        else:
            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]

            # add to root spec no need to check existance
            return self.updater.create_resource(
                root_field_spec.target_spec_name,
                'root',
                path,
                None,
                data)

    def delete(self, path):
        path = path.strip().strip('/')

        if '/' in path:
            parent_field_path = '/'.join(path.split('/')[:-1])
            resource_id = path.split('/')[-1]

            tree = parse_canonical_url(path, self.schema.root)
            parent_field_tree = parse_canonical_url(parent_field_path, self.schema.root)

            parent_path = '/'.join(parent_field_path.split('/')[:-1])
            field_name = parent_field_path.split('/')[-1]

            if type(parent_field_tree) == LinkCollectionResourceRef:
                parent_tree = parse_canonical_url(parent_path, self.schema.root)
                aggregate_query, spec, is_aggregate = parent_tree.aggregation(None)

                # if we're using a simplified parser we can probably just pull the id off the path
                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                return self.updater.delete_linkcollection_entry(
                    spec.name,
                    parent_resource['_id'],
                    field_name,
                    resource_id)
            else:
                aggregate_query, spec, is_aggregate = parent_field_tree.aggregation(None)

                return self.updater.delete_resource(spec.name, resource_id, spec.name, field_name)
        else:
            raise Exception("Cannot delete root resource")

    def get(self, path):
        path = path.strip().strip('/')
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

    def get_spec_for(self, path):
        path = path.strip().strip('/')
        tree = parse_url(path, self.schema.root)

        aggregate_query, spec, is_aggregate = tree.aggregation(None)
        return (
            spec,
            is_aggregate,
            type(tree) in (CollectionResourceRef,
                           RootResourceRef),
            type(tree) in (LinkCollectionResourceRef,),
        )

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
            if field.field_type == 'reverse_link':
                aggregate_query.append(
                    {"$lookup": {
                            "from": "resource_%s" % field.target_spec_name,
                            "localField": "_id",
                            "foreignField": field.reverse_link_field,
                            "as": "_expanded_%s" % field_name,
                    }})
        return aggregate_query

    def encode_resource(self, spec, resource_data):
        self_url = os.path.join(
            resource_data['_parent_canonical_url'],
            resource_data['_parent_field_name'],
            self.schema.encodeid(resource_data['_id']))
        encoded = {
            'id': self.schema.encodeid(resource_data['_id']),
            'self': self_url,
        }
        for field_name, field in spec.fields.items():
            field_value = resource_data.get(field_name)
            if field.field_type == 'link':
                if field_value:
                    encoded[field_name] = resource_data['_canonical_url_%s' % field_name]
                else:
                    encoded[field_name] = None
            elif field.field_type == 'parent_collection':
                encoded[field_name] = resource_data['_parent_canonical_url']
            elif field.field_type in ('linkcollection', 'collection', 'reverse_link', 'reverse_link_collection'):
                encoded[field_name] = os.path.join(self_url, field_name)
            elif field.field_type == 'calc':
                tree = parse(field.calc_str, spec)
                res_type = tree.infer_type()
                calc_result = resource_data[field_name]
                if res_type.is_primitive():
                    if tree.is_collection():
                        encoded[field_name] = [res[res_type.name] for res in calc_result]
                    else:
                        encoded[field_name] = calc_result
                elif tree.is_collection():
                    encoded[field_name] = os.path.join(self_url, field_name)
                else:
                    encoded[field_name] = self.encode_resource(tree.infer_type(), calc_result)
            else:
                encoded[field_name] = field_value
        return encoded

    def search_resource(self, spec_name, query_str):
        spec = self.schema.specs[spec_name]
        if query_str:
            query = parse_filter(query_str, spec)
            query = query.condition_aggregation(spec)
        else:
            query = {}
        results = self.schema.db['resource_%s' % spec_name].find(query)
        resources = []
        for result in results:
            resource_id = self.schema.encodeid(result['_id'])
            resources.append(self.encode_resource(spec, result))
        return resources
