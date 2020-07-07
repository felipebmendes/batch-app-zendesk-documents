from ..functions.ingestion import data_ingestion
from ..flow.commons import Task
import luigi
import pandas as pd
from pycarol.pipeline import ParquetTarget
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


class IngestDocuments(Task):

    target_type = ParquetTarget
    entity_documents = luigi.Parameter()
    entity_name_documents = luigi.Parameter()
    connector_name = luigi.Parameter(default=None)
    columns_documents = luigi.ListParameter(default=None)
    env_name = luigi.ListParameter(default=None)
    org_name = luigi.ListParameter(default=None)
    max_workers = luigi.Parameter(default=None, significant=False)
    merge_records = luigi.ListParameter(default=True)

    def easy_run(self, inputs):
        out = data_ingestion(
            name=self.entity_name_documents,
            entity=self.entity_documents, connector_name=self.connector_name, columns=self.columns_documents,
            env_name=self.env_name, org_name=self.org_name, max_workers=self.max_workers, merge_records=self.merge_records,
        )
        return out


class IngestUsers(Task):

    target_type = ParquetTarget
    entity_users = luigi.Parameter()
    entity_name_users = luigi.Parameter()
    connector_name = luigi.Parameter(default=None)
    columns_users = luigi.ListParameter(default=None)
    env_name = luigi.ListParameter(default=None)
    org_name = luigi.ListParameter(default=None)
    max_workers = luigi.Parameter(default=None, significant=False)
    merge_records = luigi.ListParameter(default=True)

    def easy_run(self, inputs):
        out = data_ingestion(
            name=self.entity_name_users,
            entity=self.entity_users, connector_name=self.connector_name, columns=self.columns_users,
            env_name=self.env_name, org_name=self.org_name, max_workers=self.max_workers, merge_records=self.merge_records,
        )
        return out


class IngestCategories(Task):

    target_type = ParquetTarget
    entity_categories = luigi.Parameter()
    entity_name_categories = luigi.Parameter()
    connector_name = luigi.Parameter(default=None)
    columns_categories = luigi.ListParameter(default=None)
    env_name = luigi.ListParameter(default=None)
    org_name = luigi.ListParameter(default=None)
    max_workers = luigi.Parameter(default=None, significant=False)
    merge_records = luigi.ListParameter(default=True)

    def easy_run(self, inputs):
        out = data_ingestion(
            name=self.entity_name_categories,
            entity=self.entity_categories, connector_name=self.connector_name, columns=self.columns_categories,
            env_name=self.env_name, org_name=self.org_name, max_workers=self.max_workers, merge_records=self.merge_records,
        )
        return out


class IngestSections(Task):

    target_type = ParquetTarget
    entity_sections = luigi.Parameter()
    entity_name_sections = luigi.Parameter()
    connector_name = luigi.Parameter(default=None)
    columns_sections = luigi.ListParameter(default=None)
    env_name = luigi.ListParameter(default=None)
    org_name = luigi.ListParameter(default=None)
    max_workers = luigi.Parameter(default=None, significant=False)
    merge_records = luigi.ListParameter(default=True)

    def easy_run(self, inputs):
        out = data_ingestion(
            name=self.entity_name_sections,
            entity=self.entity_sections, connector_name=self.connector_name, columns=self.columns_sections,
            env_name=self.env_name, org_name=self.org_name, max_workers=self.max_workers, merge_records=self.merge_records,
        )
        return out