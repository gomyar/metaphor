
import os
import re
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime

from flask import request

from metaphor.lrparse.lrparse import parse
from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url
from metaphor.lrparse.lrparse import parse_filter
from metaphor.lrparse.lrparse import CollectionResourceRef
from metaphor.lrparse.lrparse import RootResourceRef
from metaphor.lrparse.lrparse import LinkCollectionResourceRef
from metaphor.lrparse.lrparse import OrderedCollectionResourceRef
from metaphor.schema import CalcField
from metaphor.updater import Updater
from bson.errors import InvalidId

from werkzeug.security import generate_password_hash


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
    def __init__(self, schema):
        self.schema = schema
        self.updater = Updater(schema)

    @staticmethod
    def _has_grants(url_path, canonical_url, grants):
        url_path = re.sub(r'\[.*?]', '', url_path.strip('/'))
        canonical_url = canonical_url.strip('/')
        def match_grant(url, grant_url, recurse=False):
            match_re = grant_url.replace('/*', '\/ID[0-9a-f]*')
            if recurse and match_re != '/': # allow for root (admin) grant
                match_re = match_re + r'(/.*)?$'
            if not recurse:
                match_re = match_re + r'$'
            return re.match(match_re, url)

        if url_path.split('/')[0] == 'ego':
            return any(match_grant('/'+url_path, grant['url']) for grant in grants)
        else:
            return any(match_grant('/'+canonical_url, grant['url'], True) for grant in grants)

    def _check_grants(self, path, canonical_path, grants):
        if not Api._has_grants(path, canonical_path, grants):
            raise HTTPError('', 403, "Not Allowed", None, None)

    def _check_expand_ego_grants(self, path, expand_dict, grants):
        if expand_dict:
            for field_name, nested in expand_dict.items():
                nested_url = os.path.join(path, field_name)
                if Api._has_grants(nested_url, '', grants) or self._check_expand_ego_grants(nested_url, expand_dict[field_name], grants):
                    return True
        return False

    def patch(self, path, data, user=None):
        path = path.strip().strip('/')
        try:
            tree = parse_canonical_url(path, self.schema.root)
        except SyntaxError as te:
            raise HTTPError('', 404, "Not Found", None, None)

        aggregate_query, spec, is_aggregate = tree.aggregation(None)

        if is_aggregate:
            raise HTTPError('', 400, 'PATCH not supported on collections', None, None)

        if path.split('/')[0] == 'ego':
            aggregate_query = [
                {"$match": {"username": user.username}}
            ] + aggregate_query

        cursor = tree.root_collection().aggregate(aggregate_query)

        resource = next(cursor)

        if user:
            # TODO: also need to check read access to target if link
            # checking for /ego paths first, then all other paths
            self._check_grants(path, resource['_canonical_url'], user.update_grants)

        if spec.name == 'user' and data.get('password'):
            data['password'] = generate_password_hash(data['password'])

        return self.updater.update_fields(
            spec.name,
            self.schema.encodeid(resource['_id']),
            data)

    def put(self, path, data, user=None):
        path = path.strip().strip('/')
        from_path = data['_from'].strip('/') if data.get('_from') else None
        at_index = data.get('_at')

        if '/' in path:
            parent_path, field_name = path.rsplit('/', 1)
            try:
                tree = parse_canonical_url(parent_path, self.schema.root)
            except SyntaxError as te:
                raise HTTPError('', 404, "Not Found", None, None)

            aggregate_query, spec, is_aggregate = tree.aggregation(None)

            field_spec = spec.fields[field_name]

            if path.split('/')[0] == 'ego':
                aggregate_query = [
                    {"$match": {"username": user.username}}
                ] + aggregate_query

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            # check permissions
            if user:
                self._check_grants(path, os.path.join(parent_resource['_canonical_url'], field_name), user.put_grants)
                self._check_grants(from_path, from_path, user.read_grants)
                self._check_grants(from_path, from_path, user.delete_grants)

            # do put update
            try:
                parse_url(from_path, self.schema.root)
            except SyntaxError as te:
                raise HTTPError('', 400, from_path, None, None)

            if field_spec.field_type == 'collection':
                return self.updater.move_resource(parent_resource['_id'], spec.name, field_name, path, from_path)
            else:
                raise HTTPError('', 400, from_path, None, None)

        else:
            if path not in self.schema.root.fields:
                raise HTTPError('', 404, "Not Found", None, None)

            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]

            # check permissions
            if user:
                self._check_grants(path, path, user.put_grants)
                self._check_grants(from_path, from_path, user.read_grants)
                self._check_grants(from_path, from_path, user.delete_grants)

            if root_field_spec.target_spec_name == 'user':
                data['password'] = generate_password_hash(data['password'])

            # do put update
            try:
                if from_path:
                    parse_url(from_path, self.schema.root)
            except SyntaxError as te:
                raise HTTPError('', 400, from_path, None, None)

            return self.updater.move_resource(None, 'root', path, path, from_path)

    def post(self, path, data, user=None):
        path = path.strip().strip('/')

        if '/' in path:
            parent_path, field_name = path.rsplit('/', 1)
            try:
                tree = parse_canonical_url(parent_path, self.schema.root)
            except SyntaxError as te:
                raise HTTPError('', 404, "Not Found", None, None)

            aggregate_query, spec, is_aggregate = tree.aggregation(None)

            field_spec = spec.fields[field_name]

            if path.split('/')[0] == 'ego':
                aggregate_query = [
                    {"$match": {"username": user.username}}
                ] + aggregate_query

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            # check permissions
            if user:
                # TODO: also need to check read access to target if link
                # checking for /ego paths first, then all other paths
                self._check_grants(path, os.path.join(parent_resource['_canonical_url'], field_name), user.create_grants)

            parent_id = self.schema.encodeid(parent_resource['_id'])

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
                    data,
                    self.schema.read_root_grants(path))
            else:
                return self.updater.create_resource(
                    field_spec.target_spec_name,
                    spec.name,
                    field_name,
                    parent_id,
                    data,
                    self.schema.read_root_grants(path))
        else:
            if path not in self.schema.root.fields:
                raise HTTPError('', 404, "Not Found", None, None)

            root_field_spec = self.schema.root.fields[path]
            root_spec = self.schema.specs[root_field_spec.target_spec_name]

            # check permissions
            if user:
                self._check_grants(path, path, user.create_grants)

            if root_field_spec.target_spec_name == 'user':
                data['password'] = generate_password_hash(data['password'])

            # add to root spec no need to check existance
            return self.updater.create_resource(
                root_field_spec.target_spec_name,
                'root',
                path,
                None,
                data,
                self.schema.read_root_grants(path))

    def delete(self, path, user=None):
        path = path.strip().strip('/')

        if '/' in path:
            parent_field_path = '/'.join(path.split('/')[:-1])
            resource_id = path.split('/')[-1]

            try:
                tree = parse_canonical_url(path, self.schema.root)
            except ValueError as ve:
                raise HTTPError('', 404, "Not Found", None, None)

            parent_field_tree = parse_canonical_url(parent_field_path, self.schema.root)

            parent_path = '/'.join(parent_field_path.split('/')[:-1])
            field_name = parent_field_path.split('/')[-1]

            if type(parent_field_tree) == LinkCollectionResourceRef:
                parent_tree = parse_canonical_url(parent_path, self.schema.root)
                aggregate_query, spec, is_aggregate = parent_tree.aggregation(None)

                if path.split('/')[0] == 'ego':
                    aggregate_query = [
                        {"$match": {"username": user.username}}
                    ] + aggregate_query

                # if we're using a simplified parser we can probably just pull the id off the path
                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                if user:
                    # checking for /ego paths first, then all other paths
                    self._check_grants(parent_field_path, parent_resource['_canonical_url'], user.delete_grants)

                return self.updater.delete_linkcollection_entry(
                    spec.name,
                    parent_resource['_id'],
                    field_name,
                    resource_id)
            elif type(parent_field_tree) == OrderedCollectionResourceRef:
                parent_tree = parse_canonical_url(parent_path, self.schema.root)
                aggregate_query, spec, is_aggregate = parent_tree.aggregation(None)

                if path.split('/')[0] == 'ego':
                    aggregate_query = [
                        {"$match": {"username": user.username}}
                    ] + aggregate_query

                # if we're using a simplified parser we can probably just pull the id off the path
                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                if user:
                    self._check_grants(parent_field_path, parent_resource['_canonical_url'], user.delete_grants)

                return self.updater.delete_orderedcollection_entry(
                    spec.name,
                    parent_resource['_id'],
                    field_name,
                    resource_id)
            else:
                aggregate_query, spec, is_aggregate = parent_field_tree.aggregation(None)

                if path.split('/')[0] == 'ego':
                    aggregate_query = [
                        {"$match": {"username": user.username}}
                    ] + aggregate_query

                cursor = tree.root_collection().aggregate(aggregate_query)
                parent_resource = next(cursor)

                if user:
                    self._check_grants(parent_field_path, parent_resource['_canonical_url'], user.delete_grants)

                parent_spec_name = parent_field_tree.parent_spec.name if parent_field_tree.parent_spec else None
                return self.updater.delete_resource(spec.name, resource_id, parent_spec_name, field_name)
        else:
            raise HTTPError('', 400, "Cannot delete root resource", None, None)

    def _get_root(self):
        root_resource = {
            'auth': '/auth',
            'ego': '/ego',
            'users': '/users',
            'groups': '/groups',
            'employees': '/employees',
            'divisions': '/division',
        }
        return root_resource

    def get(self, path, args=None, user=None):
        args = args or {}
        expand = args.get('expand')
        page = int(args.get('page', 0))
        page_size = int(args.get('page_size', 10))

        expand_dict = create_expand_dict(expand)

        path = path.strip().strip('/')
        if not path:
            return self._get_root()

        try:
            tree = parse_url(path, self.schema.root)
        except SyntaxError as te:
            return None

        aggregate_query, spec, is_aggregate = tree.aggregation(None)

        if path.split('/')[0] == 'ego':

            aggregate_query = [
                {"$match": {"username": user.username}}
            ] + aggregate_query


        if is_aggregate:

            page_agg = self.create_pagination_aggregations(page, page_size)

            if expand:
                if user and self._check_expand_ego_grants(path, expand_dict, user.read_grants):
                    expand_agg = self.create_field_expansion_aggregations(spec, expand_dict)
                else:
                    expand_agg = self.create_field_expansion_aggregations(spec, expand_dict, user)
                page_agg['$facet']["results"].extend(expand_agg)

            aggregate_query.append(page_agg)

            # run mongo query from from root_resource collection
            cursor = tree.root_collection().aggregate(aggregate_query)

            page_results = next(cursor)

            results = list(page_results['results'])
            count = page_results['count'][0]['total'] if page_results['count'] else 0

            if user and count:
                # TODO: also need to check read access to target if link
                # checking for /ego paths first, then all other paths
                self._check_grants(path, results[0]['_canonical_url'], user.read_grants)

            return {
                "results": [self.encode_resource(spec, row, expand_dict) for row in results],
                "count": count,
                "next": self._next_link(path, args, count, page, page_size),
                "previous": self._previous_link(path, args, count, page, page_size),
                '_meta': {
                    'spec': {
                        'name': spec.name,
                    },
                    'is_collection': True,
                    'can_create': type(tree) in (CollectionResourceRef,
                                                 OrderedCollectionResourceRef,
                                               RootResourceRef),
                    'can_link': type(tree) in (LinkCollectionResourceRef,),
                }
            }

        else:
            if expand:
                if user and self._check_expand_ego_grants(path, expand_dict, user.read_grants):
                    expand_agg = self.create_field_expansion_aggregations(spec, expand_dict)
                else:
                    expand_agg = self.create_field_expansion_aggregations(spec, expand_dict, user)
                aggregate_query.extend(expand_agg)

            # run mongo query from from root_resource collection
            cursor = tree.root_collection().aggregate(aggregate_query)
            result = next(cursor, None)

            if user and result:
                # TODO: also need to check read access to target if link
                # checking for /ego paths first, then all other paths
                self._check_grants(path, result['_canonical_url'], user.read_grants)

            if result:
                return self.encode_resource(spec, result, expand_dict)
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

    def get_spec_for(self, path, user=None):
        path = path.strip().strip('/')
        tree = parse_url(path, self.schema.root)

        aggregate_query, spec, is_aggregate = tree.aggregation(None, user)
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

    def create_field_expansion_aggregations(self, spec, expand_dict, user=None):

        def lookup_agg(from_field, local_field, foreign_field, as_field, expand_further):
            agg = {"$lookup": {
                "from": from_field,
                "as": as_field,
                "let": {
                    "v_id": "$%s" % local_field,
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$$v_id", "$%s" % foreign_field]
                            }
                        }
                    }
                ]
            }}
            for inner_field_name in expand_further:
                inner_spec = spec.schema.specs[spec.fields[inner_field_name].target_spec_name]
                agg['$lookup']['pipeline'].extend(self.create_field_expansion_aggregations(inner_spec, expand_further[inner_field_name], user))
            return agg

        def lookup_collection_agg(from_field, local_field, foreign_field, as_field, expand_further):
            agg = {"$lookup": {
                "from": from_field,
                "as": as_field,
                "let": {
                    "v_id": "$%s" % local_field,
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$%s" % foreign_field, "$$v_id"]
                            }
                        }
                    }
                ]
            }}
            for inner_field_name in expand_further:
                inner_spec = spec.schema.specs[spec.fields[inner_field_name].target_spec_name]
                agg['$lookup']['pipeline'].extend(self.create_field_expansion_aggregations(inner_spec, expand_further[inner_field_name], user))
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
                        "resource_%s" % field.target_spec_name,
                        field_name,
                        "_id",
                        "_expanded_%s" % field_name,
                        expand_dict))
                aggregate_query.append(
                    {"$unwind": "$_expanded_%s" % field_name}
                )
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type == 'reverse_link':
                aggregate_query.append(
                    lookup_agg(
                            "resource_%s" % field.target_spec_name,
                            "_id",
                            field.reverse_link_field,
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$unwind": "$_expanded_%s" % field_name}
                )
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            elif field.field_type in ('linkcollection', 'orderedcollection'):
                aggregate_query.append(
                    lookup_collection_agg(
                            "resource_%s" % field.target_spec_name,
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
                            "resource_%s" % field.target_spec_name,
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
                            "resource_%s" % spec.name,
                            "_parent_id",
                            "_id",
                            "_expanded_%s" % field_name,
                            expand_dict
                    ))
                aggregate_query.append(
                    {"$set": {field_name: "$_expanded_%s" % field_name}}
                )
            else:
                raise HTTPError('', 400, 'Unable to expand field %s of type %s' % (field_name, field.field_type), None, None)
        if user:
            aggregate_query.append(
                {"$match": {"_grants": {"$in": user.grants}}}
            )
        return aggregate_query

    def encode_resource(self, spec, resource_data, expand_dict):

        self_url = os.path.join(
            resource_data['_parent_canonical_url'],
            resource_data['_parent_field_name'],
            self.schema.encodeid(resource_data['_id']))
        encoded = {
            'id': self.schema.encodeid(resource_data['_id']),
            'self': self_url,
            '_meta': {
                'spec': {
                    'name': spec.name,
                },
                'is_collection': False,
            }
        }
        for field_name, field in spec.fields.items():
            field_value = resource_data.get(field_name)
            if field.field_type == 'link':
                if field_value:
                    if field_name in expand_dict:
                        encoded[field_name] = self.encode_resource(self.schema.specs[field.target_spec_name], resource_data[field_name], expand_dict[field_name])
                    else:
                        encoded[field_name] = resource_data['_canonical_url_%s' % field_name]
                else:
                    encoded[field_name] = None
            elif field.field_type == 'parent_collection' and resource_data.get('_parent_id'):
                encoded[field_name] = resource_data['_parent_canonical_url']
            elif field.field_type in ('reverse_link',):
                if field_name in expand_dict:
                    encoded[field_name] = self.encode_resource(self.schema.specs[field.target_spec_name], resource_data[field_name], expand_dict[field_name])
                else:
                    # TODO: A canonical link would be better
                    encoded[field_name] = os.path.join(self_url, field_name)
            elif field.field_type in ('linkcollection', 'collection', 'reverse_link_collection', 'orderedcollection'):
                if field_name in expand_dict:
                    encoded[field_name] = [self.encode_resource(self.schema.specs[field.target_spec_name], citem, expand_dict[field_name]) for citem in resource_data[field_name]]
                else:
                    encoded[field_name] = os.path.join(self_url, field_name)
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
                    encoded[field_name] = os.path.join(self_url, field_name)
                else:
                    encoded[field_name] = resource_data['_canonical_url_%s' % field.name] if calc_result else None
            elif spec.name == 'user' and field_name == 'password':
                encoded[field_name] = '<password>'
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
            {"$match": query},
            pagination,
        ]

        cursor = self.schema.db['resource_%s' % spec_name].aggregate(aggregation)
        page_results = next(cursor)

        results = list(page_results['results'])
        count = page_results['count'][0]['total'] if page_results['count'] else 0

        return {
            "results": [self.encode_resource(spec, row, {}) for row in results],
            "count": count,
            "next": self._next_link(None, {}, count, page, page_size),
            "previous": self._previous_link(None, {}, count, page, page_size),
            '_meta': {
                'spec': {
                    'name': spec.name,
                },
                'is_collection': True,
            }
        }
