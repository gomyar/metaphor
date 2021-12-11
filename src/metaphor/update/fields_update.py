

class FieldsUpdate:
    def __init__(self, updater, schema, spec_name, resource_id, fields):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.resource_id = resource_id
        self.fields = fields

    def execute(self):
        spec = self.schema.specs[self.spec_name]
        self.schema.update_resource_fields(self.spec_name, self.resource_id, self.fields)

        # update local resource calcs
        #   update dependent field calcs (involving fields)
        # update foreign resource calcs (involving fields)

        # recalc local calcs
        # moved this ahead of the code to recalc foreign calcs
        # there may be an opportunity to check whether the change only affects the current resource
        # (if there are only "self." references in the calc)
        # this will stop the reverse aggregation, which finds itself only and becomes a no-op
        # careful with multi layered calcs within the same resource
        for field_name, field_spec in spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(spec.name, field_name, self.resource_id)
                self.updater._recalc_for_field_update(spec, spec.name, field_name, self.resource_id)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for fields
            for field_name in self.fields:
                field_dep = "%s.%s" % (self.spec_name, field_name)
                if field_dep in calc_tree.get_resource_dependencies():
                    self.updater._perform_updates_for_affected_calcs(spec, self.resource_id, calc_spec_name, calc_field_name)



