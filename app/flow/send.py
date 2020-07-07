from ..functions.send import send_data_to_carol
from .commons import Task
from . import etl
from . import ingestion
import luigi
from pycarol.pipeline import inherit_list
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


@inherit_list(
    etl.DocumentsPreparation,
)
class SendDocuments(Task):

    staging_documents = luigi.Parameter()
    connector_documents = luigi.Parameter()

    def easy_run(self, inputs):

        return send_data_to_carol(
            staging_name=self.staging_documents,
            connector_name=self.connector_documents,
            df=inputs[0]
        )


@inherit_list(
    ingestion.IngestUsers,
)
class SendUsers(Task):

    staging_users = luigi.Parameter()
    connector_users = luigi.Parameter()

    def easy_run(self, inputs):

        return send_data_to_carol(
            staging_name=self.staging_users,
            connector_name=self.connector_users,
            df=inputs[0]
        )
