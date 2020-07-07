import logging
from pycarol import Carol, Staging

logger = logging.getLogger(__name__)



def send_data_to_carol(staging_name, connector_name,  df):

    staging = Staging(Carol())
    staging.send_data(
        staging_name=staging_name, connector_name=connector_name, data=df, async_send=True,
        storage_only=True,
    )
