

class CreateSpecMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name):
        self.updater = updater
        self.to_schema = to_schema

        self.spec_name = spec_name

    def __repr__(self):
        return "<CreateSpecMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        pass
