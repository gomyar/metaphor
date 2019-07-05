
import unittest

from .lrfilter import aggregate_from_filter


class LrFilterTest(unittest.TestCase):
    def test_basic_aggregate(self):
        aggregate_chain = aggregate_from_filter('name="Fred"')
        self.assertEquals({'$match': {'name': 'Fred'}}, aggregate_chain)

        aggregate_chain = aggregate_from_filter('name="Fred",age=12')
        self.assertEquals({'$match': {'name': 'Fred', 'age': 12}}, aggregate_chain)

    def test_aggregate_from_resourceref(self):
        aggregate_chain = aggregate_from_resource_ref("employees[age=40].certs")

        self.assertEquals([
            {'$match': {'age': 40}},
            {'$lookup': {'from': 'certs', 'as': 'employees__certs'}},
            {'$unwind': '$employees__certs'},
        ], aggregate_chain)

    def test_aggregate_from_resourceref_2(self):
        aggregate_chain = aggregate_from_resource_ref("employees[age=40].certs[name='firstaid']")

        self.assertEquals([
            {'$match': {'age': 40}},
            {'$lookup': {'from': 'certs', 'as': 'employees__certs'}},
            {'$unwind': '$employees__certs'},
            {'$match': {'employees__certs.name': 'firstaid'}},
        ], aggregate_chain)

    def test_aggregate_from_resourceref_3(self):
        aggregate_chain = aggregate_from_resource_ref("employees[age=40].certs[name='firstaid']")

        self.assertEquals([
            {'$match': {'age': 40}},
            {'$lookup': {'from': 'certs', 'as': 'employees__certs'}},
            {'$unwind': '$employees__certs'},
            {'$match': {'employees__certs.name': 'firstaid'}},
        ], aggregate_chain)

