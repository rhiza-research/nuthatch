import pandas as pd
import dask.dataframe as dd
import dask_deltatable as ddt
import dask_deltatable.utils as _ddt_utils
from deltalake import DeltaTable, write_deltalake
from nuthatch.backend import FileBackend, register_backend
import fsspec

import logging
logger = logging.getLogger(__name__)

# Workaround for dask_deltatable bug: maybe_set_aws_credentials doesn't
# recognize fsspec's key/secret format, causing it to fall through to a
# boto3 import path that injects invalid kwargs into storage_options.
# Related: https://github.com/dask-contrib/dask-deltatable/issues/88
# TODO: file upstream issue and remove this patch once fixed.
_original_maybe_set_aws_credentials = _ddt_utils.maybe_set_aws_credentials


def _patched_maybe_set_aws_credentials(path, options):
    if options and ('key' in options or 'secret' in options):
        return options
    return _original_maybe_set_aws_credentials(path, options)


_ddt_utils.maybe_set_aws_credentials = _patched_maybe_set_aws_credentials

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


def _remap_s3_options(options):
    """Remap fsspec S3 options to deltalake format."""
    remapped = {}

    # First pass through any keys already in deltalake format
    aws_deltalake_keys = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_ENDPOINT_URL',
                          'AWS_SESSION_TOKEN', 'AWS_REGION', 'AWS_DEFAULT_REGION',
                          'allow_http', 'AWS_ALLOW_HTTP']
    for key in aws_deltalake_keys:
        if key in options:
            remapped[key] = options[key]

    # Map fsspec keys to deltalake AWS keys (only if not already present)
    if 'AWS_ACCESS_KEY_ID' not in remapped and 'key' in options:
        remapped['AWS_ACCESS_KEY_ID'] = options['key']
    if 'AWS_SECRET_ACCESS_KEY' not in remapped and 'secret' in options:
        remapped['AWS_SECRET_ACCESS_KEY'] = options['secret']

    # Handle endpoint_url: check top-level first, then nested in client_kwargs
    if 'AWS_ENDPOINT_URL' not in remapped:
        if 'endpoint_url' in options:
            remapped['AWS_ENDPOINT_URL'] = options['endpoint_url']
        elif 'client_kwargs' in options and isinstance(options['client_kwargs'], dict):
            if 'endpoint_url' in options['client_kwargs']:
                remapped['AWS_ENDPOINT_URL'] = options['client_kwargs']['endpoint_url']

    # Auto-enable HTTP for non-HTTPS endpoints (e.g., LocalStack)
    endpoint_url = remapped.get('AWS_ENDPOINT_URL', '')
    if endpoint_url.startswith('http://') and 'allow_http' not in remapped:
        remapped['allow_http'] = 'true'

    if 'AWS_SESSION_TOKEN' not in remapped and 'session_token' in options:
        remapped['AWS_SESSION_TOKEN'] = options['session_token']

    return remapped


_PROTOCOL_REMAPPERS = {
    's3': _remap_s3_options,
    's3a': _remap_s3_options,
    'gs': _remap_gcs_options,
    'gcs': _remap_gcs_options,
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
