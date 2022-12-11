

class FieldsUpdate:
    def __init__(self, updater, schema, spec_name, resource_id, fields):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.resource_id = resource_id
        self.fields = fields

    def execute(self):
        update_id = str(self.schema.create_update())

        spec = self.schema.specs[self.spec_name]
        self.schema.update_resource_fields(self.spec_name, self.resource_id, self.fields)

        # find and update dependent calcs
        start_agg = [
            {"$match": {"_id": self.schema.decodeid(self.resource_id)}}
        ]

        for field_name in self.fields:
            self.updater.update_for_field(self.spec_name, field_name, update_id, start_agg)

        self.schema.cleanup_update(update_id)
