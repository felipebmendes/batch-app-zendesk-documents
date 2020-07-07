from pycarol import Carol, DataModel, Staging, Apps, PwdAuth
import os
import logging

logger = logging.getLogger(__name__)


def data_ingestion(entity, name, connector_name=None, columns=None, env_name=None,
                   org_name=None, callback=None, max_workers=None, merge_records=True,
                   carol=None):
    """
    Ingests data model from Carol.

    Parameters
    ----------

    entity:  string
        Ingestion from which entity. Possible values:
            1. `staging`
            2.  `data_model`

    name: string
        Name of table or data model.

    connector_name: string
        Connector name, if entity==staging

    columns: list of strings or None (default=None)
        Columns we want to return.
        If None, all columns will be returned.

    callback: callable
        Function which accepts the data model as input and also returns a data model
        with the same columns, perhaps after applying some transformation,
        such as a row filter.

    merge_records: bool (default=True)
        Merge records after download.
    max_workers: int (default=None)
        Whether or not to multiprocess to ingest data.

    carol: pycarol.Carol() (default=None)
        Instance of pycarol.Carol() to be used to access the tables. If note it will initialize from env variables.

    Returns
    -------

    df: Pandas DataFrame
        Data model.
    """

    # login

    if columns is not None:
        columns = list(columns)  # luigi send as a tuple.

    if carol is None:
        carol = Carol()

    if entity == 'staging':
        if connector_name is None:
            raise ValueError('connector_name must be set for entity==staging')

        if env_name is not None:
            if os.environ.get('FLASK_ENV') == 'development':
                # Running locally
                carol = Carol(env_name, 'asd', auth=PwdAuth(os.environ['CAROLUSER'], os.environ['CAROLPWD']),
                            organization=org_name)
                carol_app = Apps(carol).all()
                if carol_app:
                    carol_app, _ = carol_app.popitem()
                else:
                    print("No carol app")
                    carol_app = "aaa"
                carol.app_name = carol_app
                api_key = carol.issue_api_key()
                print(api_key['X-Auth-Key'])
                print(api_key['X-Auth-ConnectorId'])
            else:
                # Running on Carol
                carol.switch_environment(env_name=env_name, app_name=' ')
                logger.debug('switching environment')
                logger.debug(carol.get_current())

        stag = Staging(carol)
        df = stag.fetch_parquet(staging_name=name, connector_name=connector_name, backend='pandas', callback=callback,
                                columns=columns, merge_records=merge_records, cds=True, max_workers=max_workers)
    elif entity == 'data_model':
        dm = DataModel(carol)
        df = dm.fetch_parquet(dm_name=name, backend='pandas',  columns=columns,  callback=callback,
                              merge_records=merge_records,  cds=True,
                              max_workers=max_workers)
    else:
        raise ValueError(f'entity must be `staging` or `data_model` entered {entity}')

    return df
