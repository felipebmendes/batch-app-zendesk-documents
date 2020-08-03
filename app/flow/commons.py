import luigi

luigi.interface.InterfaceLogging.setup(luigi.interface.core())

import os
import logging
import traceback

logger = logging.getLogger(__name__)

from pycarol.pipeline import Task
from luigi import Parameter

PROJECT_PATH = os.getcwd()
TARGET_PATH = os.path.join(PROJECT_PATH, 'luigi_targets')
Task.TARGET_DIR = TARGET_PATH
#Change here to save targets locally.
Task.is_cloud_target = False
Task.version = Parameter()
Task.resources = {'cpu': 1}

# Exec params

# Documents
entity_documents = 'staging'
entity_name_documents = 'articles'
connector_name = 'carol_connect_zerado'
body_column = 'body'

# Users
entity_users = 'staging'
entity_name_users = 'users'

# Sections
entity_sections = 'staging'
entity_name_sections = 'sections'

# Categories
entity_categories = 'staging'
entity_name_categories = 'categories'

env_name = 'monitoriaqa'
org_name = 'totvs'

named_query_name = 'get_documents'
id_column_name = 'id'
filter_column_name = 'section'
app_name = 'searchlgpddocuments'
url = 'https://lgpdassistant-searchlgpddocuments.apps.carol.ai/update_embeddings'

#Results params

#Documents
staging_documents = 'documents'
connector_documents = 'zendesk'

#Users
staging_users = 'users'
connector_users = 'zendesk'


@Task.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    logger.error(f'Error msg: {exception} ---- Error: Task {task},')
    traceback_str = ''.join(traceback.format_tb(exception.__traceback__))
    logger.error(traceback_str)


@Task.event_handler(luigi.Event.PROCESSING_TIME)
def print_execution_time(self, processing_time):
    logger.debug(f'### PROCESSING TIME {processing_time}s. Output saved at {self.output().path}')


#######################################################################################################



params = dict(
    version=os.environ.get('CAROLAPPVERSION', 'dev'),

    entity_documents = entity_documents,
    entity_name_documents = entity_name_documents,
    connector_name = connector_name,
    
    entity_users = entity_users,
    entity_name_users = entity_name_users,
    body_column = body_column,

    entity_sections = entity_sections,
    entity_name_sections = entity_name_sections,

    entity_categories = entity_categories,
    entity_name_categories = entity_name_categories,
    
    env_name = env_name,
    org_name = org_name,

    staging_documents = staging_documents,
    connector_documents = connector_documents,
    
    staging_users = staging_users,
    connector_users = connector_users,

    named_query_name = named_query_name,
    id_column_name =  id_column_name,
    filter_column_name = filter_column_name,
    app_name=app_name,
    url = url

)
