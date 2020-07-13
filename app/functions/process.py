import logging
from pycarol import Carol, CDSStaging

logger = logging.getLogger(__name__)



def process_data(staging_name, connector_name):

    cds = CDSStaging(Carol())
    cds.process_data(staging_name=staging_name, connector_name=connector_name)
