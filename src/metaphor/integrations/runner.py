
import time
import importlib
import uuid

from pymongo import ReturnDocument
from pymongo import MongoClient
import gevent

from metaphor.integrations.mongo_change_stream import MongoChangeStreamIntegration

import logging
log = logging.getLogger(__name__)


class IntegrationRunner:
    def __init__(self, api, db):
        self.api = api
        self.db = db
        self.running = True
        self.running_integrations = {}
        self.gthread_id = str(uuid.uuid4())

    def start(self):
        log.info("Starting integration runner")
        self.gthread = gevent.spawn(self._run)

    def stop(self):
        log.info("Stopping integration runner")
        self.running = False

        log.debug("integrations: %s", self.running_integrations)
        for integration, _ in self.running_integrations.values():
            integration.stream.close()
        # join all in self.running_integrations
        log.debug("Integration runnner waiting on running integrations")
        gthreads = [g[1] for g in self.running_integrations.values()]
        log.debug("gthreads: %s", gthreads)
        gevent.joinall(gthreads)
        # join self & exit
        self.gthread.join()
        log.info("Integration runner stopped")

    def _run(self):
        while self.running:
            integration = self.db['metaphor_integrations'].find_one_and_update(
                {'state': 'starting'},
                {'$set': {'state': 'running', 'runner_gthread_id': self.gthread_id, 'gthread_id': str(uuid.uuid4())}},
                return_document=ReturnDocument.AFTER)

            if integration:
                log.info("Starting integration : %s", integration.get('name'))
                # create new gthread (change_stream) and run
                change_stream = self._create_change_stream(integration)
                gthread = gevent.spawn(self._run_integration, change_stream, integration)
                self.running_integrations[integration['gthread_id']] = (change_stream, gthread)
                log.debug("running_integrations: %s", self.running_integrations)

            stopping_integration = self.db['metaphor_integrations'].find_one_and_update(
                {'state': 'stopping', 'runner_gthread_id': self.gthread_id},
                {'$set': {'state': 'pending_stop'}},
                return_document=ReturnDocument.AFTER)

            if stopping_integration:
                log.info("Stopping integration : %s", stopping_integration.get('name'))
                change_stream, gthread = self.running_integrations.pop(stopping_integration['gthread_id'])
                change_stream.stream.close()
                gthread.join()

                stopping_integration = self.db['metaphor_integrations'].update_one(
                    {'gthread_id': stopping_integration['gthread_id']},
                    {'$set': {'state': 'stopped'}})

            time.sleep(2)

    def _create_change_stream(self, integration):
        source_client = MongoClient(integration['mongo_connection'])
        source_db = source_client[integration['mongo_db']]
        callback_module_name, callback_func_name = integration['change_stream_callback'].rsplit('.', 1)
        change_stream_callback_module = importlib.import_module(callback_module_name)
        change_stream_callback_func = getattr(change_stream_callback_module, callback_func_name)

        return MongoChangeStreamIntegration(
            source_db,
            integration['mongo_collection'],
            integration['mongo_aggregation'],
            self.api,
            change_stream_callback_func)

    def _run_integration(self, change_stream, integration):
        log.info("Running...")
        try:
            change_stream.process()

            log.info("Stream ended")
            self.db['metaphor_integrations'].update({'_id': integration['_id']}, {'$set': {'state': 'stopped'}})
        except Exception as e:
            log.exception("Exception running integration: %s", integration.get('name'))
            self.db['metaphor_integrations'].update({'_id': integration['_id']}, {'$set': {'state': 'error', 'error_info': {'exception': str(e)}}})
