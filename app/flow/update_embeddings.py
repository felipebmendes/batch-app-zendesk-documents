from ..functions.update_embeddings import update_embeddings
from .commons import Task
from . import process
import luigi
from pycarol.pipeline import inherit_list
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


@inherit_list(
    process.ProcessDocuments,
)
class UpdateEmbeddings(Task):

    named_query_name = luigi.Parameter()
    id_column_name = luigi.Parameter()
    filter_column_name = luigi.Parameter()
    url = luigi.Parameter()

    def easy_run(self, inputs):

        return update_embeddings(
            named_query_name=self.named_query_name,
            id_column_name=self.id_column_name,
            filter_column_name=self.filter_column_name,
            url=self.url
        )
