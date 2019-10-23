
import unittest

from pymongo import MongoClient
from bson.objectid import ObjectId

from metaphor.resource import ResourceSpec, FieldSpec, CollectionSpec
from metaphor.resource import ResourceLinkSpec, CalcSpec
from metaphor.resource import LinkCollectionSpec
from metaphor.schema import Schema
from metaphor.api import MongoApi
from metaphor.schema_factory import SchemaFactory

from datetime import datetime
from mock import patch


class SpikeTest(unittest.TestCase):
    def setUp(self):
        client = MongoClient()
        client.drop_database('metaphor_test_db')
        self.db = client.metaphor_test_db
        self.schema = Schema(self.db, '0.1')

        self.employee_spec = ResourceSpec('employee')
        self.job_spec = ResourceSpec('job')

        self.schema.add_resource_spec(self.employee_spec)
        self.schema.add_resource_spec(self.job_spec)

        self.employee_spec.add_field("name", FieldSpec("str"))
        self.employee_spec.add_field("jobs", CollectionSpec('job'))
        self.employee_spec.add_field("total_kg", CalcSpec('sum(self.jobs.kg)', 'int'))
        self.employee_spec.add_field("total_tonnes", CalcSpec('sum(self.jobs.tonnes)', 'int'))
        self.employee_spec.add_field("total_debt", CalcSpec('100 + sum(self.jobs.tonnes)', 'int'))
        self.employee_spec.add_field("total_fuel", CalcSpec('sum(self.jobs.fuel)', 'int'))

        self.job_spec.add_field("name", FieldSpec("str"))
        self.job_spec.add_field("tonnes", FieldSpec("int"))
        self.job_spec.add_field("fuel", FieldSpec("int"))
        self.job_spec.add_field("kg", CalcSpec("self.tonnes * 1000", 'int'))
        self.job_spec.add_field("grams", CalcSpec("self.kg * 1000", "int"))  # localsecondary update

        self.schema.add_root('employees', CollectionSpec('employee'))

        self.api = MongoApi('server', self.schema, self.db)

        self.employee_id = self.api.post('employees', {'name': 'Bob'})
        self.job_id = self.api.post('employees/%s/jobs' % (self.employee_id,), {'name': 'move_coal', 'tonnes': 16})

    def test_all_local_deps(self):
        job = self.api.build_resource('employees/%s/jobs/%s' % (self.employee_id, self.job_id))
        data = {}
        job._follow_local_dependencies(['tonnes'], data)
        self.assertEquals({'kg': 16000.0, 'grams': 16000000.0}, data)

    def test_assert_defaults(self):
        employee = self.api.get('employees/%s' % (self.employee_id,))
        self.assertEquals(16, employee['total_tonnes'])
        self.assertEquals(16000, employee['total_kg'])
        self.assertEquals(116, employee['total_debt'])

    def test_affected_resources(self):
        job = self.api.build_resource('employees/%s/jobs/%s' % (self.employee_id, self.job_id))

        found = self.schema.updater.find_affected_calcs_for_field(self.job_spec.fields['tonnes'])

        self.assertEquals(set([
            (self.job_spec.fields['kg'], 'self.tonnes', 'self'),
            (self.employee_spec.fields['total_tonnes'], 'self.jobs.tonnes', 'self.jobs'),
            (self.employee_spec.fields['total_debt'], 'self.jobs.tonnes', 'self.jobs'),
        ]), found)

        resource_ids = self.schema.updater.find_altered_resource_ids(found, job)
        self.assertEquals(set([
            ('job', 'kg', (self.job_id,)),
            ('employee', 'total_tonnes', (self.employee_id,)),
            ('employee', 'total_debt', (self.employee_id,)),
        ]), resource_ids)



    def test_update_mongo_once_then_construct_updater(self):
        self.api.patch("employees/%s/jobs/%s" % (self.employee_id, self.job_id), {'tonnes': 32, 'fuel': 1})
        # send > 1 fields to be updated

        # assert single update sent to mongo

        # assert updater(s) refers to dependencies only
