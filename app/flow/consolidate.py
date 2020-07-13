from ..functions.consolidate import consolidate_staging
from .commons import Task
from . import send
import luigi
from pycarol.pipeline import inherit_list
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


@inherit_list(
    send.SendDocuments,
)
class ConsolidateDocuments(Task):

    staging_documents = luigi.Parameter()
    connector_documents = luigi.Parameter()

    def easy_run(self, inputs):

        return consolidate_staging(
            staging_name=self.staging_documents,
            connector_name=self.connector_documents
        )


@inherit_list(
    send.SendUsers,
)
class ConsolidateUsers(Task):

    staging_users = luigi.Parameter()
    connector_users = luigi.Parameter()

    def easy_run(self, inputs):

        return consolidate_staging(
            staging_name=self.staging_users,
            connector_name=self.connector_users
        )
