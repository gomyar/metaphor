
from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url

class MoveLinkUpdate:
    def __init__(self, updater, schema, parent_id, field_name, from_path=None, at_index=None):
        self.updater = updater
        self.schema = schema

        self.parent_id = parent_id
        self.field_name = field_name
        self.from_path = from_path
        self.at_index = at_index

    def execute(self):
        from_tree = parse_url(self.from_path, self.schema.root)

        aggregate_query, spec, is_aggregate = from_tree.aggregation(None)
        update_ids = [r['_id'] for r in from_tree.root_collection().aggregate(aggregate_query)]

        # add to new linkcollection

        # remove from original linkcollection

        # TODO: perform updates for create / delete


        return None
