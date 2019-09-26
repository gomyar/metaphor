
import unittest
import json

from pymongo import MongoClient

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.api import SchemaApi
from metaphor.api import RootsApi
from metaphor.api_bp import api_bp
from metaphor.schema_bp import schema_bp

from flask import Flask


class SchemaUpdateTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db

        self.schema = Schema(self.db, "1.1")

        self.app = Flask(__name__)
        self.app.secret_key = "1234test"

        self.app.config['api'] = MongoApi('http://server', self.schema, self.db)
        self.app.config['schema'] = self.schema

        self.app.register_blueprint(api_bp)
        self.app.register_blueprint(schema_bp)

        self.client = self.app.test_client()

    def test_add_resource(self):
        self.assertEquals(None, self.db['metaphor_schema'].find_one())
        self.assertEquals(1, len(self.schema.specs))
        self.client.post('schema/specs', data=json.dumps({'name': 'company'}), content_type='application/json')
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals({
            'company': {'fields': {}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_add_resource_with_fields(self):
        self.assertEquals(1, len(self.schema.specs))
        self.client.post('schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        self.assertEquals(2, len(self.schema.specs))

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('str', self.schema.specs['company'].fields['name'].field_type)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'}}, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_update_resource_spec(self):
        self.client.post('schema/specs', data=json.dumps({'name': 'company'}), content_type='application/json')
        self.client.patch('schema/specs/company', data=json.dumps({'assets': {'type': 'int'}}), content_type='application/json')

        self.assertEquals('company', self.schema.specs['company'].name)
        self.assertEquals('int', self.schema.specs['company'].fields['assets'].field_type)

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
            {'company': {'fields': {'assets': {'type': 'int'}}, 'type': 'resource'}
            }, saved_schema['specs'])
        self.assertEquals({}, saved_schema['roots'])

    def test_add_root_and_post(self):
        resp = self.client.post('/schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        resp = self.client.post('api/companies', data=json.dumps({'name': 'Fred'}), content_type='application/json')

        saved_schema = self.db['metaphor_schema'].find_one()
        self.assertEquals(
             {'company': {'fields': {'name': {'type': 'str'}}, 'type': 'resource'}},
             saved_schema['specs'])
        self.assertEquals({'companies': {'type': 'collection', 'target': 'company'}}, saved_schema['roots'])

    def test_patch_and_put(self):
        resp = self.client.post('/schema/specs', data=json.dumps({'name': 'company', 'fields': {'name': {'type': 'str'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        resp = self.client.post('api/companies', data=json.dumps({'name': 'Fred'}), content_type='application/json')
        company_id = json.loads(resp.data)['id']
        resp = self.client.put('api/companies/%s' % (company_id,), data=json.dumps({'name': 'Ned'}), content_type='application/json')

        ned = self.db['resource_company'].find_one()
        self.assertEquals(
            'Ned',
            ned['name'])

        resp = self.client.patch('api/companies/%s' % (company_id,), data=json.dumps({'name': 'Neddy'}), content_type='application/json')

        ned = self.db['resource_company'].find_one()
        self.assertEquals(
            'Neddy',
            ned['name'])

    def test_recalc_on_update(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.post('/api/companies', data=json.dumps({'name': 'Bobs Burgers', 'assets': 100, 'liabilities': 80}), content_type='application/json')
        company_id_1 = json.loads(response.data)['id']
        response = self.client.post('/api/companies', data=json.dumps({'name': 'Neds Fries', 'assets': 50, 'liabilities': 40}), content_type='application/json')
        company_id_2 = json.loads(response.data)['id']

        self.client.patch('/schema/specs/company', data=json.dumps(
            {'profit': {'type': 'calc', 'calc': 'self.assets - self.liabilities', 'calc_type': 'int'}}), content_type='application/json')

        response = self.client.get('/api/companies/%s' % (company_id_1,))
        company_1 = json.loads(response.data)
        response = self.client.get('/api/companies/%s' % (company_id_2,))
        company_2 = json.loads(response.data)

        self.assertEquals(20, company_1['profit'])
        self.assertEquals(10, company_2['profit'])

    def test_cannot_add_invalid_calc(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'profit': {'type': 'calc', 'calc': 'self.NONEXISTANT - self.liabilities', 'calc_type': 'int'}}), content_type='application/json')

        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "u'NONEXISTANT'"}, json.loads(response.data))

    def test_cannot_add_invalid_type(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'profit': {'type': 'nonexistant'}}), content_type='application/json')

        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "u'nonexistant'"}, json.loads(response.data))

    def test_cannot_add_reserved_word(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'self': {'type': 'nonexistant'}}), content_type='application/json')
        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "u'nonexistant'"}, json.loads(response.data))

    def test_cannot_add_link_prefixed_field(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'link_something': {'type': 'company'}}), content_type='application/json')
        self.assertEquals(400, response.status_code)
        self.assertEquals({u'error': u"Fields cannot start with 'link_' (reserved for interal use)"}, json.loads(response.data))

    def test_cannot_add_underscore_prefixed(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'_something': {'type': 'company'}}), content_type='application/json')
        self.assertEquals(400, response.status_code)
        self.assertEquals({u'error': u"Fields cannot start with '_' (reserved for interal use)"}, json.loads(response.data))

    def test_cannot_add_circular_dependency(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {'name': {'type': 'str'}, 'assets': {'type': 'int'}, 'liabilities': {'type': 'int'}}}), content_type='application/json')
        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'allAssets': {'type': 'calc', 'calc': 'self.assets', 'calc_type': 'int'}}), content_type='application/json')
        self.assertEquals(200, response.status_code)

        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'totalAssets': {'type': 'calc', 'calc': 'self.allAssets', 'calc_type': 'int'}}), content_type='application/json')
        self.assertEquals(200, response.status_code)

        # re-patch allAssets to refer to totalAssets
        response = self.client.patch('/schema/specs/company', data=json.dumps(
            {'allAssets': {'type': 'calc', 'calc': 'self.totalAssets', 'calc_type': 'int'}}), content_type='application/json')
        self.assertEquals(400, response.status_code)
        self.assertEquals({"error": "Cyclic dependencies exist among these items: (u'company.allAssets', set([u'company.totalAssets'])), (u'company.totalAssets', set([u'company.allAssets']))"}, json.loads(response.data))

    def test_cannot_add_circular_dependency_from_other_resources(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {
                'name': {'type': 'str'},
                'assets': {'type': 'int'},
            }}), content_type='application/json')
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'other_company', 'fields': {
                'name': {'type': 'str'},
                'company': {'type': 'link', 'target': 'company'},
                'company_assets': {'type': 'calc', 'calc': 'self.company.assets', 'calc_type': 'int'},
            }}), content_type='application/json')

        self.assertEquals(200, resp.status_code)

        resp = self.client.patch('/schema/specs/company', data=json.dumps(
            {'other_company': {'type': 'link', 'target': 'other_company'},
            }), content_type='application/json')
        resp = self.client.patch('/schema/specs/company', data=json.dumps(
            {'assets': {'type': 'calc', 'calc': 'self.other_company.company_assets', 'calc_type': 'int'},
            }), content_type='application/json')

        self.assertEquals(400, resp.status_code)
        self.assertEquals(
            {u'error': u"Cyclic dependencies exist among these items: (u'company.assets', set([u'other_company.company_assets'])), (u'other_company.company_assets', set([u'company.assets']))"},
            json.loads(resp.data))

    def test_circular_dependency_with_self(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {
                'assets': {'type': 'int'},
                'assets_plus_one': {'type': 'calc', 'calc': 'self.assets + 1', 'calc_type': 'int'},
                'assets_plus_four': {'type': 'calc', 'calc': 'self.assets_plus_four+ 1', 'calc_type': 'int'},
            }}), content_type='application/json')

        self.assertEquals(400, resp.status_code)
        self.assertEquals({"error": "Calc cannot depend on itself: company.assets_plus_four"}, json.loads(resp.data))

    def test_interdependent_fields_added_together(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {
                'assets': {'type': 'int'},
                'assets_plus_four': {'type': 'calc', 'calc': 'self.assets_plus_three + 1', 'calc_type': 'int'},
                'assets_plus_three': {'type': 'calc', 'calc': 'self.assets_plus_two + 1', 'calc_type': 'int'},
                'assets_plus_two': {'type': 'calc', 'calc': 'self.assets_plus_one + 1', 'calc_type': 'int'},
                'assets_plus_one': {'type': 'calc', 'calc': 'self.assets + 1', 'calc_type': 'int'},
            }}), content_type='application/json')

        self.assertEquals(200, resp.status_code)

        resp = self.client.post('/schema/root', data=json.dumps({'name': 'companies', 'target': 'company'}), content_type='application/json')
        self.assertEquals(200, resp.status_code)

        post_resp = self.client.post('/api/companies', data=json.dumps({'assets': 1}), content_type='application/json')
        self.assertEquals(200, post_resp.status_code)
        company_id_1 = json.loads(post_resp.data)['id']

        response = self.client.get('/api/companies/%s' % (company_id_1,))
        company = json.loads(response.data)

        self.assertEquals(1, company['assets'])
        self.assertEquals(2, company['assets_plus_one'])
        self.assertEquals(3, company['assets_plus_two'])
        self.assertEquals(4, company['assets_plus_three'])
        self.assertEquals(5, company['assets_plus_four'])

    def test_include_spec_in_api_call(self):
        self.client.post('/schema/specs', data=json.dumps(
            {'name': 'employer', 'fields': {
                'name': {'type': 'str'},
            }}), content_type='application/json')

        self.client.post('/schema/root', data=json.dumps({'name': 'employers', 'target': 'employer'}), content_type='application/json')

        resp = self.client.get("/schema/specfor/employers", content_type='application/json')
        self.assertEquals(200, resp.status_code)

        employer_spec = json.loads(resp.data)
        self.assertEquals(
            {'spec': 'collection',
             'target_spec': {
                'name': 'employer',
                'spec': 'resource',
                'fields': {
                    'link_root_employers': {
                        'name': 'root.employers',
                        'spec': 'reverse_link',
                        'type': 'reverse_link',
                    },
                    'name': {
                        'spec': 'field', 'type': 'str',
                    },
                },
            },
            'type': 'collection'},
            employer_spec)

    def test_interdependent_fields_added_together_specific(self):
        resp = self.client.post('/schema/specs', data=json.dumps(
            {'name': 'company', 'fields': {
                'assets': {'type': 'int'},
                'assets_plus_one': {'type': 'calc', 'calc': 'self.assets + 1', 'calc_type': 'int'},
            }}), content_type='application/json')
        self.assertEquals(200, resp.status_code)
