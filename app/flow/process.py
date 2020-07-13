from ..functions.process import process_data
from .commons import Task
from . import consolidate
import luigi
from pycarol.pipeline import inherit_list
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


@inherit_list(
    consolidate.ConsolidateDocuments,
)
class ProcessDocuments(Task):

    staging_documents = luigi.Parameter()
    connector_documents = luigi.Parameter()

    def easy_run(self, inputs):

        return process_data(
            staging_name=self.staging_documents,
            connector_name=self.connector_documents
        )


@inherit_list(
    consolidate.ConsolidateUsers,
)
class ProcessUsers(Task):

    staging_users = luigi.Parameter()
    connector_users = luigi.Parameter()

    def easy_run(self, inputs):

        return process_data(
            staging_name=self.staging_users,
            connector_name=self.connector_users
        )
