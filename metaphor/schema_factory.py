

from metaphor.resource import ResourceSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CalcSpec
from metaphor.resource import FieldSpec
from metaphor.resource import CollectionSpec
from metaphor.schema import Schema


class SchemaFactory(object):
    def create_schema(self, db, version, data):
        schema = Schema(db, version)
        return schema
