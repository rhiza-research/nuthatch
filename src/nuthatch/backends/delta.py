import pandas as pd
import dask.dataframe as dd
import dask_deltatable as ddt
from deltalake import DeltaTable, write_deltalake
from nuthatch.backend import FileBackend, register_backend
import fsspec

import logging
logger = logging.getLogger(__name__)

def _remap_gcs_options(options):
    """Remap fsspec GCS options to deltalake format.

    For emulator testing, use 'google_service_account' (reads gcs_base_url from JSON).
    GOOGLE_APPLICATION_CREDENTIALS also works but doesn't read gcs_base_url.
    """
    remapped = {}

    # First pass through any keys already in deltalake format
    gcs_deltalake_keys = ['google_service_account', 'GOOGLE_APPLICATION_CREDENTIALS',
                          'GOOGLE_CLOUD_PROJECT', 'skip_signature']
    for key in gcs_deltalake_keys:
        if key in options:
            remapped[key] = options[key]

    # Map fsspec keys to deltalake GCS keys (only if not already present)
    if 'google_service_account' not in remapped and 'GOOGLE_APPLICATION_CREDENTIALS' not in remapped:
        if 'service_account_file' in options:
            remapped['google_service_account'] = options['service_account_file']
        elif 'token' in options:
            token = options['token']
            # Only map token if it's a file path string (not a special keyword or object)
            # gcsfs special keywords: 'anon', 'google_default', 'browser', 'cache', 'cloud'
            if isinstance(token, str) and token not in ('anon', 'google_default', 'browser', 'cache', 'cloud'):
                remapped['google_service_account'] = token
    if 'GOOGLE_CLOUD_PROJECT' not in remapped and 'project' in options:
        remapped['GOOGLE_CLOUD_PROJECT'] = options['project']

    return remapped

def _remap_azure_options(options):
    """Remap fsspec Azure options to deltalake format."""
    remapped = {}

    # First pass through any keys already in deltalake format
    azure_deltalake_keys = ['AZURE_STORAGE_ACCOUNT_NAME', 'AZURE_STORAGE_ACCOUNT_KEY',
                            'AZURE_SAS_TOKEN', 'AZURE_STORAGE_CONNECTION_STRING',
                            'AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET',
                            'AZURE_ENDPOINT_SUFFIX', 'azure_storage_use_emulator',
                            'azure_storage_endpoint', 'allow_http']
    for key in azure_deltalake_keys:
        if key in options:
            remapped[key] = options[key]

    # Map fsspec keys to deltalake Azure keys (only if not already present)
    if 'AZURE_STORAGE_ACCOUNT_NAME' not in remapped and 'account_name' in options:
        remapped['AZURE_STORAGE_ACCOUNT_NAME'] = options['account_name']
    if 'AZURE_STORAGE_ACCOUNT_KEY' not in remapped and 'account_key' in options:
        remapped['AZURE_STORAGE_ACCOUNT_KEY'] = options['account_key']
    if 'AZURE_SAS_TOKEN' not in remapped and 'sas_token' in options:
        remapped['AZURE_SAS_TOKEN'] = options['sas_token']
    if 'AZURE_TENANT_ID' not in remapped and 'tenant_id' in options:
        remapped['AZURE_TENANT_ID'] = options['tenant_id']
    if 'AZURE_CLIENT_ID' not in remapped and 'client_id' in options:
        remapped['AZURE_CLIENT_ID'] = options['client_id']
    if 'AZURE_CLIENT_SECRET' not in remapped and 'client_secret' in options:
        remapped['AZURE_CLIENT_SECRET'] = options['client_secret']

    # Parse connection_string for account credentials
    if 'connection_string' in options and 'AZURE_STORAGE_ACCOUNT_NAME' not in remapped:
        conn_str = options['connection_string']
        for part in conn_str.split(';'):
            if part.startswith('AccountName=') and 'AZURE_STORAGE_ACCOUNT_NAME' not in remapped:
                remapped['AZURE_STORAGE_ACCOUNT_NAME'] = part.split('=', 1)[1]
            elif part.startswith('AccountKey=') and 'AZURE_STORAGE_ACCOUNT_KEY' not in remapped:
                remapped['AZURE_STORAGE_ACCOUNT_KEY'] = part.split('=', 1)[1]
            elif part.startswith('BlobEndpoint='):
                endpoint = part.split('=', 1)[1]
                if endpoint.startswith('http://') and 'allow_http' not in remapped:
                    remapped['allow_http'] = 'true'
                if 'azure_storage_endpoint' not in remapped:
                    remapped['azure_storage_endpoint'] = endpoint

    return remapped

_PROTOCOL_REMAPPERS = {
    'gs': _remap_gcs_options,
    'gcs': _remap_gcs_options,
    'az': _remap_azure_options,
    'abfs': _remap_azure_options,
    'abfss': _remap_azure_options,
}


@register_backend
class DeltaBackend(FileBackend):
    """
    Delta backend for caching tabular data in a delta table.

    This backend supports dask and pandas dataframes.
    """

    backend_name = "delta"
    default_for_type = pd.DataFrame

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, extension='delta')

        # Remap filesystem_options to be compatible with write_deltalake.
        # deltalake's Rust backend uses different key names than fsspec.
        protocol = fsspec.utils.get_protocol(self.path)
        remapper = _PROTOCOL_REMAPPERS.get(protocol)
        if remapper:
            self._delta_storage_options = remapper(self.filesystem_options)
        else:
            self._delta_storage_options = self.filesystem_options.copy()

    def write(self, data):
        """Write a pandas dataframe to a delta table."""
        if isinstance(data, dd.DataFrame):
            logger.warning("""Warning: Dask datafame passed to delta backend. Will run `compute()`
                      on the dataframe prior to storage. This will fail if the dataframe
                      does not fit in memory. Use `backend=parquet` to handle parallel writing of dask dataframes.""")
            write_data = data.compute()
        elif isinstance(data, pd.DataFrame):
            write_data = data
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")

        write_deltalake(self.path, write_data, mode='overwrite', schema_mode='overwrite', storage_options=self._delta_storage_options)
        return data


    def upsert(self, data, upsert_keys=None):
        raise NotImplementedError("Delta backend does not support upsert.")


    def read(self, engine=None):
        if engine == 'pandas' or engine == pd.DataFrame or engine is None:
            return DeltaTable(self.path, storage_options=self._delta_storage_options).to_pandas()
        elif engine == 'dask' or engine == dd.DataFrame:
            return ddt.read_deltalake(
                self.path,
                storage_options=self.filesystem_options,
                delta_storage_options=self._delta_storage_options,
            )
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")
