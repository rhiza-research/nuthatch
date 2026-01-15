from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask, pandas_df
import pandas as pd
from nuthatch.config import config_parameter
from google.cloud import secretmanager

@config_parameter('password', location='root')
def postgres_write_password():
    """Get a postgres write password."""
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/sheerwater-postgres-write-password/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key


def test_sql():
    multi_tab_test_pandas(backend='sql')
    multi_tab_test_dask(backend='sql')

# Test sql as storage backend
def test_sql_storage_backend():
    df = pandas_df(name="oak", backend='delta', storage_backend='sql', cache_mode='overwrite')
    df2 = pandas_df(name="oak", backend='sql')
    pd.testing.assert_frame_equal(df, df2)
