import pandas as pd
import dask.dataframe as dd
from deltalake import DeltaTable, write_deltalake
from nuthatch.backend import FileBackend, register_backend
import fsspec

import logging
logger = logging.getLogger(__name__)

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

        # Remap filesystem_options to be compatible with write_deltalake
        # Detect storage provider from path protocol
        protocol = fsspec.utils.get_protocol(self.path)

        # Get original filesystem_options (already set by parent class)
        original_options = self.config.get('filesystem_options', {}).copy()
        remapped_options = {}

        if protocol in ('s3', 's3a'):
            # AWS S3: First pass through any keys already in deltalake format
            aws_deltalake_keys = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_ENDPOINT_URL',
                                  'AWS_SESSION_TOKEN', 'AWS_REGION', 'AWS_DEFAULT_REGION',
                                  'allow_http', 'AWS_ALLOW_HTTP']
            for key in aws_deltalake_keys:
                if key in original_options:
                    remapped_options[key] = original_options[key]

            # Then map fsspec keys to deltalake AWS keys (only if not already present)
            if 'AWS_ACCESS_KEY_ID' not in remapped_options and 'key' in original_options:
                remapped_options['AWS_ACCESS_KEY_ID'] = original_options['key']
            if 'AWS_SECRET_ACCESS_KEY' not in remapped_options and 'secret' in original_options:
                remapped_options['AWS_SECRET_ACCESS_KEY'] = original_options['secret']

            # Handle endpoint_url: check top-level first, then nested in client_kwargs
            if 'AWS_ENDPOINT_URL' not in remapped_options:
                if 'endpoint_url' in original_options:
                    remapped_options['AWS_ENDPOINT_URL'] = original_options['endpoint_url']
                elif 'client_kwargs' in original_options and isinstance(original_options['client_kwargs'], dict):
                    if 'endpoint_url' in original_options['client_kwargs']:
                        remapped_options['AWS_ENDPOINT_URL'] = original_options['client_kwargs']['endpoint_url']

            # Auto-enable HTTP for non-HTTPS endpoints (e.g., LocalStack)
            endpoint_url = remapped_options.get('AWS_ENDPOINT_URL', '')
            if endpoint_url.startswith('http://') and 'allow_http' not in remapped_options:
                remapped_options['allow_http'] = 'true'

            if 'AWS_SESSION_TOKEN' not in remapped_options and 'session_token' in original_options:
                remapped_options['AWS_SESSION_TOKEN'] = original_options['session_token']

        elif protocol in ('gs', 'gcs'):
            # Google Cloud Storage: First pass through any keys already in deltalake format
            # NOTE: For emulator testing, use 'google_service_account' (reads gcs_base_url from JSON).
            # GOOGLE_APPLICATION_CREDENTIALS also works but doesn't read gcs_base_url.
            gcs_deltalake_keys = ['google_service_account', 'GOOGLE_APPLICATION_CREDENTIALS',
                                  'GOOGLE_CLOUD_PROJECT', 'skip_signature']
            for key in gcs_deltalake_keys:
                if key in original_options:
                    remapped_options[key] = original_options[key]

            # Then map fsspec keys to deltalake GCS keys (only if not already present)
            # Use google_service_account to enable gcs_base_url support for emulator testing
            if 'google_service_account' not in remapped_options and 'GOOGLE_APPLICATION_CREDENTIALS' not in remapped_options:
                if 'service_account_file' in original_options:
                    remapped_options['google_service_account'] = original_options['service_account_file']
                elif 'token' in original_options:
                    token = original_options['token']
                    # Only map token if it's a file path string (not a special keyword or object)
                    # gcsfs special keywords: 'anon', 'google_default', 'browser', 'cache', 'cloud'
                    if isinstance(token, str) and token not in ('anon', 'google_default', 'browser', 'cache', 'cloud'):
                        # Looks like a file path - map it
                        remapped_options['google_service_account'] = token
            if 'GOOGLE_CLOUD_PROJECT' not in remapped_options and 'project' in original_options:
                remapped_options['GOOGLE_CLOUD_PROJECT'] = original_options['project']

        elif protocol in ('az', 'abfs', 'abfss'):
            # Azure Blob Storage: First pass through any keys already in deltalake format
            azure_deltalake_keys = ['AZURE_STORAGE_ACCOUNT_NAME', 'AZURE_STORAGE_ACCOUNT_KEY',
                                   'AZURE_SAS_TOKEN', 'AZURE_STORAGE_CONNECTION_STRING',
                                   'AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET',
                                   'AZURE_ENDPOINT_SUFFIX', 'azure_storage_use_emulator',
                                   'azure_storage_endpoint', 'allow_http']
            for key in azure_deltalake_keys:
                if key in original_options:
                    remapped_options[key] = original_options[key]

            # Then map fsspec keys to deltalake Azure keys (only if not already present)
            if 'AZURE_STORAGE_ACCOUNT_NAME' not in remapped_options and 'account_name' in original_options:
                remapped_options['AZURE_STORAGE_ACCOUNT_NAME'] = original_options['account_name']
            if 'AZURE_STORAGE_ACCOUNT_KEY' not in remapped_options and 'account_key' in original_options:
                remapped_options['AZURE_STORAGE_ACCOUNT_KEY'] = original_options['account_key']
            if 'AZURE_SAS_TOKEN' not in remapped_options and 'sas_token' in original_options:
                remapped_options['AZURE_SAS_TOKEN'] = original_options['sas_token']
            if 'AZURE_STORAGE_CONNECTION_STRING' not in remapped_options and 'connection_string' in original_options:
                remapped_options['AZURE_STORAGE_CONNECTION_STRING'] = original_options['connection_string']
            if 'AZURE_TENANT_ID' not in remapped_options and 'tenant_id' in original_options:
                remapped_options['AZURE_TENANT_ID'] = original_options['tenant_id']
            if 'AZURE_CLIENT_ID' not in remapped_options and 'client_id' in original_options:
                remapped_options['AZURE_CLIENT_ID'] = original_options['client_id']
            if 'AZURE_CLIENT_SECRET' not in remapped_options and 'client_secret' in original_options:
                remapped_options['AZURE_CLIENT_SECRET'] = original_options['client_secret']
        else:
            # For other protocols (file, etc.), pass through as-is
            remapped_options = original_options.copy()

        # Store remapped options separately - don't modify shared config
        # The original filesystem_options are used by metastore/fsspec
        self._delta_storage_options = remapped_options

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


        write_deltalake(self.path, write_data, mode='overwrite', schema_mode='overwrite', storage_options=self._delta_storage_options.copy())
        return data


    def upsert(self, data, upsert_keys=None):
        raise NotImplementedError("Delta backend does not support upsert.")


    def read(self, engine=None):
        if engine == 'pandas' or engine == pd.DataFrame or engine is None:
            return DeltaTable(self.path, storage_options=self._delta_storage_options.copy()).to_pandas()
        elif engine == 'dask' or engine == dd.DataFrame:
            return dd.from_pandas(DeltaTable(self.path, storage_options=self._delta_storage_options.copy()).to_pandas())
            #dd.from_polars(ps.read_delta(self.path, storage_options=self._delta_storage_options.copy())
            #return ddt.read_deltalake(self.path, storage_options=self._delta_storage_options.copy())
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")
