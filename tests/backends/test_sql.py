from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask
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
