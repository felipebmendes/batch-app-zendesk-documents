from ..functions.etl import normalize_articles
from .commons import Task
from . import ingestion
import luigi
from pycarol.pipeline import inherit_list
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


@inherit_list(
    ingestion.IngestDocuments,
    ingestion.IngestSections,
    ingestion.IngestCategories
)
class DocumentsPreparation(Task):
    body_column = luigi.Parameter()

    def easy_run(self, inputs):
        documents_df, sections_df, categories_df = inputs
        out = normalize_articles(documents_df=documents_df, sections_df=sections_df,
                                 categories_df=categories_df, body_column=body_column)
        return out

