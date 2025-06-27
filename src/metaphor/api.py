
import os
import re
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime

from flask import request

from gridfs import GridFS

from metaphor.lrparse.lrparse import parse
from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url
from metaphor.lrparse.lrparse import parse_filter
from metaphor.lrparse.lrparse import CollectionResourceRef
from metaphor.lrparse.lrparse import RootResourceRef
from metaphor.lrparse.lrparse import LinkCollectionResourceRef
from metaphor.lrparse.lrparse import OrderedCollectionResourceRef
from metaphor.lrparse.lrparse import FieldRef
from metaphor.schema import CalcField
from metaphor.schema_factory import SchemaFactory
from metaphor.updater import Updater
from bson.errors import InvalidId

import logging

logging.basicConfig(level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger()


def create_expand_dict(expand_str):
    expand = {}
    if expand_str:
        for expand_dot in expand_str.strip().split(','):
            if '.' in expand_dot:
                field_name, expand_further = expand_dot.strip().split('.', 1)
                if field_name in expand:
                    expand[field_name].update(create_expand_dict(expand_further))
                else:
                    expand[field_name] = create_expand_dict(expand_further)
            elif expand_dot:
                field_name = expand_dot.strip()
                if field_name not in expand:
                    expand[field_name] = {}
    return expand


class Api(object):
    def __init__(self, db):
        self.db = db
        self._schema = None

    @property
    def schema(self):
        return SchemaFactory(self.db).load_current_schema()

    @property
    def updater(self):
        return Updater(self.schema)

    def _url_to_path(self, url):
        url = url.strip('/')
        no_filters = re.sub(r'\[.*?]', '', url)
        no_trailing_ids = re.sub(r'\/ID[0-9a-f]*$', '', no_filters)
        ids_to_dots = re.sub(r'\/(ID[0-9a-f]*\/)?', '.', no_trailing_ids)
        return ids_to_dots

    def _read_grants(self, user_id, method):
        results = self.db.resource_user.aggregate([
            {"$match": {"_id": user_id}},
            {"$lookup": {
                "foreignField": "_id",
                "localField": "groups._id",
                "as": "groups",
                "from": "resource_user",
            }},
            {"$lookup": {
                "foreignField": "_parent_id",
                "localField": "groups._id",
                "as": "group_grants",
                "from": "resource_user",
            }},
            {"$limit": 1},
            {"$unwind": "$group_grants"},
            {"$match": {"group_grants.type": method}},
            {"$project": {
                "grants": "$group_grants.url",
            }},
        ])
        return [r['grants'] for r in results]

    def can_access(self, user, method, url):
        path = self._url_to_path(url)
        return path in user.grants[method] or '/' in user.grants[method]

    def can_access_expand(self, user, method, url, expand_dict):
        for field_name in expand_dict:
            if not self.can_access(user, method, os.path.join(url, field_name)):
                return False
            if expand_dict[field_name]:
                return self.can_access_expand(user, method, os.path.join(url, field_name), expand_dict[field_name])
        return True

    def patch(self, path, data, user=None):
        path = path.strip().strip('/')
        tree = self._parse_canonical_url(path)

        aggregate_query = tree.create_aggregation()
        if path[:4] == 'ego/' or path == 'ego':
            aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

        spec = tree.infer_type()
        is_aggregate = tree.is_collection()

        if is_aggregate:
            raise HTTPError('', 400, 'PATCH not supported on collections', None, None)

        cursor = tree.root_collection().aggregate(aggregate_query)

        resource = next(cursor)

        if user:
            if not self.can_access(user, "update", path):
                raise HTTPError('', 403, "Not Allowed", None, None)

        return self.updater.update_fields(
            spec.name,
            self.schema.encodeid(resource['_id']),
            data)

    def put(self, path, data, user=None):
        path = path.strip().strip('/')
        from_path = data['_from'].strip('/') if data.get('_from') else None
        at_index = data.get('_at')

        # check permissions
        if user:
            if not self.can_access(user, "put", path):
                raise HTTPError('', 403, "Not Allowed", None, None)
            if not self.can_access(user, "read", from_path):
                raise HTTPError('', 403, "Not Allowed", None, None)
            if not self.can_access(user, "delete", from_path):
                raise HTTPError('', 403, "Not Allowed", None, None)

        if '/' in path:
            parent_path, field_name = path.rsplit('/', 1)
            tree = self._parse_canonical_url(parent_path)

            aggregate_query = tree.create_aggregation()
            if path[:4] == 'ego/':
                aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

            spec = tree.infer_type()
            is_aggregate = tree.is_collection()

            field_spec = spec.fields[field_name]

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)


            # do put update
            try:
                parse_url(from_path, self.schema.root)
            except SyntaxError as te:
                raise HTTPError('', 400, from_path, None, None)

            if field_spec.field_type == 'collection':
                return self.updater.move_resource(from_path, path, parent_resource['_id'], field_name, spec.name)
            else:
                raise HTTPError('', 400, from_path, None, None)

        else:
            if path not in self.schema.root.fields:
                raise HTTPError('', 404, "Not Found", None, None)

            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]

            # do put update
            try:
                if from_path:
                    parse_url(from_path, self.schema.root)
            except SyntaxError as te:
                raise HTTPError('', 400, from_path, None, None)

            return self.updater.move_resource(from_path, path, None, path, 'root')

    def post(self, path, data, user=None, request=None):
        path = path.strip().strip('/')

        # check permissions
        if user:
            if not self.can_access(user, "create", path):
                raise HTTPError('', 403, "Not Allowed", None, None)

        if '/' in path:
            parent_path, field_name = path.rsplit('/', 1)
            tree = self._parse_canonical_url(parent_path)

            aggregate_query = tree.create_aggregation()
            if path[:4] == 'ego/':
                aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

            spec = tree.infer_type()
            is_aggregate = tree.is_collection()

            field_spec = spec.fields[field_name]

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            parent_id = self.schema.encodeid(parent_resource['_id'])

            if field_spec.field_type == 'file' and request:
                return self.updater.create_file(
                    spec.name,
                    parent_id,
                    field_name,
                    request.stream,
                    request.content_type,
                    user,
                    parent_resource.get(field_name))

            if field_spec.field_type == 'linkcollection':
                return self.updater.create_linkcollection_entry(
                    spec.name,
                    parent_id,
                    field_name,
                    data['id'])
            elif field_spec.field_type == 'orderedcollection':
                return self.updater.create_orderedcollection_entry(
                    field_spec.target_spec_name,
                    spec.name,
                    field_name,
                    parent_id,
                    data)
            else:
                return self.updater.create_resource(
                    field_spec.target_spec_name,
                    spec.name,
                    field_name,
                    parent_id,
                    data)
        else:
            if path not in self.schema.root.fields:
                raise HTTPError('', 404, "Not Found", None, None)

            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]

            # add to root spec no need to check existance
            return self.updater.create_resource(
                root_field_spec.target_spec_name,
                'root',
                path,
                None,
                data)

    def upload_file(self, path, stream, content_type, user=None):
        path = path.strip().strip('/')

        # check permissions
        if user:
            if not self.can_access(user, "create", path):
                raise HTTPError('', 403, "Not Allowed", None, None)

        if '/' in path:
            parent_path, field_name = path.rsplit('/', 1)
            tree = self._parse_canonical_url(parent_path)

            aggregate_query = tree.create_aggregation()
            if path[:4] == 'ego/':
                aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

            spec = tree.infer_type()
            is_aggregate = tree.is_collection()

            field_spec = spec.fields[field_name]
            if field_spec.field_type != 'file':
                raise HTTPError('', 400, "Field %s is not a file field" % field_name, None, None)

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            parent_id = self.schema.encodeid(parent_resource['_id'])

            return self.updater.create_file(
                spec.name,
                parent_id,
                field_name,
                stream,
                content_type,
                user)
        else:
            raise HTTPError('', 400, "Cannot upload file to root resource", None, None)

    def delete(self, path, user=None):
        path = path.strip().strip('/')

        if user:
            if not self.can_access(user, "delete", path):
                raise HTTPError('', 403, "Not Allowed", None, None)

        if '/' in path:
            parent_field_path = '/'.join(path.split('/')[:-1])
            resource_id = path.split('/')[-1]

            tree = self._parse_canonical_url(path)

            parent_field_tree = self._parse_canonical_url(parent_field_path)

            parent_path = '/'.join(parent_field_path.split('/')[:-1])
            field_name = parent_field_path.split('/')[-1]

            if type(parent_field_tree) == LinkCollectionResourceRef:
                parent_tree = self._parse_canonical_url(parent_path)

                aggregate_query = parent_tree.create_aggregation()
                if path[:4] == 'ego/':
                    aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

                spec = parent_tree.infer_type()
                is_aggregate = parent_tree.is_collection()

                # if we're using a simplified parser we can probably just pull the id off the path
                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                return self.updater.delete_linkcollection_entry(
                    spec.name,
                    parent_resource['_id'],
                    field_name,
                    resource_id)
            elif type(parent_field_tree) == OrderedCollectionResourceRef:
                parent_tree = self._parse_canonical_url(parent_path)

                aggregate_query = parent_tree.create_aggregation()
                if path[:4] == 'ego/':
                    aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

                spec = parent_tree.infer_type()
                is_aggregate = parent_tree.is_collection()

                # if we're using a simplified parser we can probably just pull the id off the path
                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                return self.updater.delete_orderedcollection_entry(
                    spec.name,
                    parent_resource['_id'],
                    field_name,
                    resource_id)

            else:
                aggregate_query = parent_field_tree.create_aggregation()
                if path[:4] == 'ego/':
                    aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

                spec = parent_field_tree.infer_type()
                is_aggregate = parent_field_tree.is_collection()

                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                # check for file deletion
                if type(tree) == FieldRef:
                    resource_tree = self._parse_canonical_url(path)
                    resource_field_name = path.rsplit('/', 1)[-1]
                    field_spec = spec.fields[resource_field_name]

                    if field_spec.field_type == 'file':
                        self.updater.delete_file(
                            spec.name,
                            parent_resource['_id'],
                            resource_field_name,
                            parent_resource[resource_field_name])
                        return {}

                parent_spec_name = parent_field_tree.parent_spec.name if parent_field_tree.parent_spec else None
                return self.updater.delete_resource(spec.name, resource_id, parent_spec_name, field_name)
        else:
            raise HTTPError('', 400, "Cannot delete root resource", None, None)

    def get(self, path, args=None, user=None):
        args = args or {}
        expand = args.get('expand')
        page = int(args.get('page', 0))
        page_size = int(args.get('page_size', 10))

        expand_dict = create_expand_dict(expand)

        path = path.strip().strip('/')

        try:
            tree = parse_url(path, self.schema.root)
        except SyntaxError as te:
            return None

        aggregate_query = tree.create_aggregation()
        if path.split('/')[0] == 'ego':
            aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

        spec = tree.infer_type()
        is_aggregate = tree.is_collection()

        if user:
            if not self.can_access(user, "read", path):
                raise HTTPError('', 403, "Not Allowed", None, None)
            if expand:
                if not self.can_access_expand(user, "read", path, expand_dict):
                    raise HTTPError('', 403, "Not Allowed", None, None)

        if is_aggregate:

            page_agg = self.create_pagination_aggregations(page, page_size)

            if expand:
                expand_agg = self.create_field_expansion_aggregations(spec, expand_dict)
                page_agg['$facet']["results"].extend(expand_agg)

            aggregate_query.append(page_agg)

            # run mongo query from from root_resource collection
            cursor = tree.root_collection().aggregate(aggregate_query)

            page_results = next(cursor)

            results = list(page_results['results'])
            count = page_results['count'][0]['total'] if page_results['count'] else 0

            return {
                "results": [self.encode_resource(spec, row, expand_dict, "resource") for row in results],
                "count": count,
                "page": page,
                "page_size": page_size,
                "next": self._next_link(path, args, count, page, page_size),
                "previous": self._previous_link(path, args, count, page, page_size),
                '_meta': {
                    'spec': {
                        'name': spec.name,
                    },
                    'is_collection': True,
                    'resource_type': tree.resource_type(),
                }
            }

        else:
            if expand:
                expand_agg = self.create_field_expansion_aggregations(spec, expand_dict)
                aggregate_query.extend(expand_agg)

            # run mongo query from from root_resource collection
            if tree.infer_type().field_type == 'file':
                parent_path, field_name = path.rsplit('/', 1)
                parent_tree = self._parse_canonical_url(parent_path)

                parent_agg = parent_tree.create_aggregation()
                if path[:4] == 'ego/':
                    parent_agg.insert(0, {"$match": {"_id": user.user_id}})

                parent_spec = parent_tree.infer_type()

                field_spec = parent_spec.fields[field_name]

                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                parent_id = self.schema.encodeid(parent_resource['_id'])
                file_id = parent_resource.get(field_name)
                if file_id:
                    fs = GridFS(self.schema.db)
                    return fs.get(file_id)
                else:
                    return None

            cursor = tree.root_collection().aggregate(aggregate_query)
            result = next(cursor, None)

            if result:
                return self.encode_resource(spec, result, expand_dict, "resource")
            else:
                return None

    def _next_link(self, path, args, count, page, page_size):
        if count >= (page + 1) * page_size:
            query = urlparse(request.url)

            new_args = dict(args)
            new_args['page'] = page + 1
            new_args['page_size'] = page_size
            new_q = urlencode(new_args)

            query = query._replace(query=new_q)
            return query.geturl()
        else:
            return None

    def _previous_link(self, path, args, count, page, page_size):
        if page:
            query = urlparse(request.url)

            new_args = dict(args)
            new_args['page'] = page - 1
            new_args['page_size'] = page_size
            new_q = urlencode(new_args)

            query = query._replace(query=new_q)
            return query.geturl()
        else:
            return None

    def get_spec_for(self, path):
        path = path.strip().strip('/')
        tree = parse_url(path, self.schema.root)

        aggregate_query = tree.create_aggregation()
        if path[:4] == 'ego/':
            aggregate_query.insert(0, {"$match": {"_id": user.user_id}})

        spec = tree.infer_type()
        is_aggregate = tree.is_collection()
        return (
            spec,
            is_aggregate,
            type(tree) in (CollectionResourceRef,
                           RootResourceRef),
            type(tree) in (LinkCollectionResourceRef,),
        )


    def create_pagination_aggregations(self, page, page_size):
        return {
            "$facet": {
                "count": [ {"$count": "total"} ],
                "results": [ {"$skip": page * page_size}, {"$limit": page_size} ],
            }
        }

    def create_field_expansion_aggregations(self, spec, expand_dict):

        def lookup_agg(spec_name, local_field, foreign_field, as_field, expand_further):
            agg = {"$lookup": {
                "from": "resource_%s" % spec_name,
                "as": as_field,
                "let": {
                    "v_id": "$%s" % local_field,
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$$v_id", "$%s" % foreign_field]},
                                    {"$eq": ["$_type", spec_name]},
                                ]
                            }
                        }
                    }
                ]
            }}
            for inner_field_name in expand_further:
                inner_spec = spec.schema.specs[spec.fields[inner_field_name].target_spec_name]
                agg['$lookup']['pipeline'].extend(self.create_field_expansion_aggregations(inner_spec, expand_further[inner_field_name]))
            return agg

        def lookup_collection_agg(spec_name, local_field, foreign_field, as_field, expand_further):
            agg = {"$lookup": {
                "from": "resource_%s" % spec_name,
                "as": as_field,
                "let": {
                    "v_id": {"$ifNull": ["$%s" % local_field, []]},
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$in": ["$%s" % foreign_field, "$$v_id"]},
                                    {"$eq": ["$_type", spec_name]},
                                ]
                            }
                        }
                    }
                ]
            }}
            for inner_field_name in expand_further:
                inner_spec = spec.schema.specs[spec.fields[inner_field_name].target_spec_name]
                agg['$lookup']['pipeline'].extend(self.create_field_expansion_aggregations(inner_spec, expand_further[inner_field_name]))
            return agg

        def lookup_reverse_link_collection_agg(spec_name, local_field, foreign_field, as_field, expand_further):
            agg = {"$lookup": {
                "from": "resource_%s" % spec_name,
                "as": as_field,
                "let": {
                    "v_id": {"$ifNull": ["$%s" % local_field, []]},
                },
                "pipeline": [
                    {"$match": {foreign_field: {"$exists": True}}},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$in": ["$$v_id", "$%s" % foreign_field]},
                                    {"$eq": ["$_type", spec_name]},
                                ]
                            }
                        }
                    }
                ]
            }}
            for inner_field_name in expand_further:
                inner_spec = spec.schema.specs[spec.fields[inner_field_name].target_spec_name]
                agg['$lookup']['pipeline'].extend(self.create_field_expansion_aggregations(inner_spec, expand_further[inner_field_name]))
            return agg

        aggregate_query = []
        for field_name in expand_dict:
            if field_name not in spec.fields:
                raise HTTPError('', 400, '%s not a field of %s' % (field_name, spec.name), None, None)
            field = spec.fields[field_name]

            if field.field_type == 'link':
                # add check for ' if in "expand" parameter'
                aggregate_query.append(
                    lookup_agg(
                        field.target_spec_name,
                        field_name,
                        "_id",
                        "_expanded_%s" % field_name,
                        expand_dict))
                aggregate_query.append(
                    {"$unwind": {"path": "$_expanded_%s" % field_name, "preserveNullAndEmptyArrays": True}}
                )
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type == 'reverse_link':
                aggregate_query.append(
                    lookup_agg(
                            field.target_spec_name,
                            "_id",
                            field.reverse_link_field,
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type in ('linkcollection','orderedcollection'):
                aggregate_query.append(
                    lookup_collection_agg(
                            field.target_spec_name,
                            "%s._id" % field.name,
                            "_id",
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type == 'collection':
                aggregate_query.append(
                    lookup_agg(
                            field.target_spec_name,
                            "_id",
                            "_parent_id",
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type == 'parent_collection':
                aggregate_query.append(
                    lookup_agg(
                            spec.fields[field_name].target_spec_name,
                            "_parent_id",
                            "_id",
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$unwind": {"path": "$_expanded_%s" % field_name, "preserveNullAndEmptyArrays": True}}
                )
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type == 'reverse_link_collection':
                aggregate_query.append(
                    lookup_reverse_link_collection_agg(
                            field.target_spec_name,
                            "_id",
                            "%s._id" % field.reverse_link_field,
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            else:
                raise HTTPError('', 400, 'Unable to expand field %s of type %s' % (field_name, field.field_type), None, None)
        return aggregate_query

    def encode_resource(self, spec, resource_data, expand_dict, resource_type):

        encoded = {
            'id': self.schema.encodeid(resource_data['_id']),
            '_meta': {
                'spec': {
                    'name': spec.name,
                },
                'is_collection': False,
                'resource_type': resource_type,
            }
        }
        for field_name, field in spec.fields.items():
            field_value = resource_data.get(field_name)
            if field.field_type == 'link':
                if field_value:
                    if field_name in expand_dict:
                        encoded[field_name] = self.encode_resource(self.schema.specs[field.target_spec_name], resource_data[field_name], expand_dict[field_name], 'link')
                    else:
                        encoded[field_name] = {"id": self.schema.encodeid(field_value)}
            elif field.field_type == 'parent_collection' and resource_data.get('_parent_id'):
                if field_name in expand_dict:
                    encoded[field_name] = self.encode_resource(self.schema.specs[field.target_spec_name], resource_data[field_name], expand_dict[field_name], 'resource')
            elif field.field_type in ('reverse_link',):
                if field_name in expand_dict:
                    encoded[field_name] = [self.encode_resource(self.schema.specs[field.target_spec_name], citem, expand_dict[field_name], 'reverse_link') for citem in resource_data[field_name]]
            elif field.field_type in ('linkcollection', 'orderedcollection', 'collection', 'reverse_link_collection',):
                if field_name in expand_dict:
                    encoded[field_name] = [self.encode_resource(self.schema.specs[field.target_spec_name], citem, expand_dict[field_name], "resource") for citem in resource_data[field_name]]
            elif field.field_type == 'calc':
                tree = parse(field.calc_str, spec)
                res_type = tree.infer_type()
                calc_result = resource_data.get(field_name)
                # TODO: Change this to url if collection always, else primitive
                if res_type.is_primitive():
                    if tree.is_collection() and calc_result is not None:
                        encoded[field_name] = calc_result
                    else:
                        encoded[field_name] = calc_result
                elif tree.is_collection():
                    pass
                else:
                    encoded[field_name] = calc_result
            else:
                encoded[field_name] = field_value
        return encoded

    def search_resource(self, spec_name, query_str, page=0, page_size=10):
        spec = self.schema.specs[spec_name]
        if query_str:
            query = parse_filter(query_str, spec)
            query = query.condition_aggregation(spec, None)
        else:
            query = {}
        pagination = self.create_pagination_aggregations(page, page_size)
        aggregation = [
            {"$match": {"_type": spec_name, "_deleted": {"$exists": False}}},
            {"$match": query},
            pagination,
        ]

        cursor = self.schema.db['resource_%s' % spec_name].aggregate(aggregation)
        page_results = next(cursor)

        results = list(page_results['results'])
        count = page_results['count'][0]['total'] if page_results['count'] else 0

        return {
            "results": [self.encode_resource(spec, row, {}, 'resource') for row in results],
            "count": count,
            "next": self._next_link(None, {}, count, page, page_size),
            "previous": self._previous_link(None, {}, count, page, page_size),
            "page": page,
            "page_size": page_size,
            '_meta': {
                'spec': {
                    'name': spec.name,
                },
                'is_collection': True,
                'resource_type': 'collection',
            }
        }

    def watch(self, url, user=None):
        # resolve resource
        path = url.strip().strip('/')
        tree = self._parse_canonical_url(path)

        aggregate_query = tree.create_aggregation()
        spec = tree.infer_type()
        is_aggregate = tree.is_collection()

        if path.split('/')[0] == 'ego':
            aggregate_query = [
                {"$match": {"_id": user.user_id}}
            ] + aggregate_query

        # establish watch
        # if single resource
        if not is_aggregate:
            log.debug("Watching resource")
            cursor = tree.root_collection().aggregate(aggregate_query)

            resource = next(cursor)

            if user:
                # TODO: also need to check read access to target if link
                # checking for /ego paths first, then all other paths
                if not self.can_access(user, "read", path):
                    raise HTTPError('', 403, "Not Allowed", None, None)

            spec_fields = spec.fields.keys()
            project_fields = dict([('document.%s' % f, '$fullDocument.%s' % f) for f in spec_fields])
            project_fields['type'] = {"$cond": {"if": {"$not": ["$fullDocument._deleted"]}, "then": "updated", "else": "deleted"}}
            project_fields['diff'] = "$updateDescription.updatedFields"
            watch_agg = [
                {"$match": {"documentKey._id": resource['_id']}},
                {"$project": project_fields},
            ]

        # if collection
        elif isinstance(tree, CollectionResourceRef) or isinstance(tree, RootResourceRef) or isinstance(tree, OrderedCollectionResourceRef):
            log.debug("Watching collection (root)")
            if '/' in path:
                parent_path, field_name = path.rsplit('/', 1)
                tree = self._parse_canonical_url(parent_path)

                aggregate_query = tree.create_aggregation()
                spec = tree.infer_type()
                is_aggregate = tree.is_collection()

                field_spec = spec.fields[field_name]

                if path.split('/')[0] == 'ego':
                    aggregate_query = [
                        {"$match": {"_id": user.user_id}}
                    ] + aggregate_query

                # if we're using a simplified parser we can probably just pull the id off the path
                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                # check permissions
                if user:
                    # TODO: also need to check read access to target if link
                    # checking for /ego paths first, then all other paths
                    if not self.can_access(user, "read", path):
                        raise HTTPError('', 403, "Not Allowed", None, None)

                # listen for changes to children of given parent
                watch_agg = [
                    {"$match": {"fullDocument._parent_id": parent_resource['_id'],
                                "fullDocument._parent_field_name": field_name}},
                ]
            else:
                log.debug("Collection watch")
                # listen for changes to children of root
                watch_agg = [
                    {"$match": {"fullDocument._parent_id": None,
                                "fullDocument._parent_field_name": path}}
                ]
            # send back type of change (update / delete / create) and details of change + diff
            spec_fields = spec.fields.keys()
            project_fields = dict([('document.%s' % f, '$fullDocument.%s' % f) for f in spec_fields])
            project_fields['type'] = {
                "$cond": {
                    "if": {
                        "$eq": ["$operationType", "insert"]
                    },
                    "then": "created",
                    "else": {"$cond": {"if": {"$not": ["$fullDocument._deleted"]}, "then": "updated", "else": "deleted"}}
                }
            }
            project_fields['diff'] = "$updateDescription.updatedFields"

            watch_agg.extend([
                {"$project": project_fields},
            ])

        # if link collection
        elif isinstance(tree, LinkCollectionResourceRef):
            log.debug("Watching link collection")
            parent_path, field_name = path.rsplit('/', 1)
            tree = self._parse_canonical_url(parent_path)

            aggregate_query = tree.create_aggregation()
            spec = tree.infer_type()
            is_aggregate = tree.is_collection()

            field_spec = spec.fields[field_name]

            if path.split('/')[0] == 'ego':
                aggregate_query = [
                    {"$match": {"_id": user.user_id}}
                ] + aggregate_query

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            # check permissions
            if user:
                # TODO: also need to check read access to target if link
                # checking for /ego paths first, then all other paths
                if not self.can_access(user, "read", path):
                    raise HTTPError('', 403, "Not Allowed", None, None)

            # only sending back updated signal, no way to get more details yet, client must reload collection
            watch_agg = [
                {"$match": {"documentKey._id": parent_resource['_id'],
                            "updateDescription.updatedFields.%s" % field_name: {"$exists": True},
                            "_type": spec.name,
                }},
                {"$project": {
                    "type": "updated",
                }},
            ]

            project_fields = dict()
            project_fields['type'] = "updated"
            project_fields['diff'] = "$updateDescription.updatedFields"
            watch_agg = [
                {"$match": {"documentKey._id": parent_resource['_id']}},
                {"$project": project_fields},
            ]

        return self.schema.db['resource_%s' % spec.name].watch(watch_agg, full_document='updateLookup')

    def _parse_canonical_url(self, path):
        try:
            return parse_canonical_url(path, self.schema.root)
        except SyntaxError as te:
            raise HTTPError('', 404, "Not Found", None, None)
