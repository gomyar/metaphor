
import pymongo

import logging
log = logging.getLogger(__name__)


class MongoChangeStreamIntegration:
    def __init__(self, source_db, collection_name, query, api, process_change_func):
        self.source_db = source_db
        self.collection_name = collection_name
        self.query = query
        self.api = api
        self.process_change_func = process_change_func
        self.stream = None

    def process(self):
        log.info("Processing mongo integration")
        self.stream = self.source_db[self.collection_name].watch(self.query)
        try:
            for change in self.stream:
                log.info("Change: %s", (change,))
                self.process_change_func(change, self.source_db, self.api)
        except pymongo.errors.OperationFailure as of:
            log.info("Mongo change stream closed")
