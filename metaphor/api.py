
import os



class MongoApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db

    def post(self, path, data):
        resource = self.build_resource(path)
        return resource.create(data)

    def patch(self, path, data):
        resource = self.build_resource(path)
        return resource.update(data)

    def get(self, path):
        path = path.strip('/')
        if path:
            resource = self.build_resource(path)
            return resource.serialize(os.path.join(self.root_url, path))
        else:
            return self.schema.root.serialize(self.root_url)

    def unlink(self, path):
        resource = self.build_resource(path)
        resource.unlink()
        return resource._id

    def build_resource(self, path):
        return self.schema.root.build_child(path)
