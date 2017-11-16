
import os

from resource import RootResource


class MongoApi(object):
    def __init__(self, root_url, schema, db):
        self.root_url = root_url
        self.schema = schema
        self.db = db
        self.root = RootResource(self)

    def post(self, path, data):
        resource = self.root.build_child(path)
        return resource.create(data)

    def patch(self, path, data):
        resource = self.root.build_child(path)
        return resource.update(data)

    def get(self, path):
        path = path.strip('/')
        if path:
            resource = self.root.build_child(path)
            return resource.serialize(os.path.join(self.root_url, path))
        else:
            return self.root.serialize(self.root_url)

    def unlink(self, path):
        resource = self.root.build_child(path)
        resource.unlink()
        return resource._id
