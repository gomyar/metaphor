
import logging
log = logging.getLogger(__name__)


def change_callback(change, source_db, api):
    log.info("Change stream event: %s", (change,))
