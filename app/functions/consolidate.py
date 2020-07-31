import logging
import random
import time
from pycarol import Carol, CDSStaging, Tasks

logger = logging.getLogger(__name__)


def consolidate_staging(staging_name, connector_name):

    login = Carol()
    cds = CDSStaging(login)
    carol_task = Tasks(login)
    task_name = f'Consolidating Staging Table: {staging_name}'
    task_id = cds.consolidate(
        staging_name=staging_name, connector_name=connector_name)
    task_id = task_id['data']['mdmId']
    while carol_task.get_task(task_id).task_status in ['READY', 'RUNNING']:
        # avoid calling task api all the time.
        time.sleep(round(12 + random.random() * 5, 2))
        logger.debug(f'Running {task_name}')
    task_status = carol_task.get_task(task_id).task_status
    if task_status == 'COMPLETED':
        logger.info(f'Task {task_id} for {connector_name}/{task_name} completed.')
    elif task_status in ['FAILED', 'CANCELED']:
        logger.error(f'Something went wrong while processing: {connector_name}/{task_name}')
        raise ValueError(f'Something went wrong while processing: {connector_name}/{task_name}')
    return 'done'
