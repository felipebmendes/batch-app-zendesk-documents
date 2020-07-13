import logging
from pycarol import Carol, Staging

logger = logging.getLogger(__name__)



def consolidate_staging(staging_name, connector_name):

    cds = CDSStaging(Carol())
    cds.consolidate(
        staging_name=staging_name, connector_name=connector_name)
