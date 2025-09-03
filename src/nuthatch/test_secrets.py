# An example import test test secret resolution for cli
from nuthatch import config_parameter

@config_parameter('password', location='root', secret=True)
def postgres_write_password():
    """Get a postgres write password."""
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/sheerwater-postgres-write-password/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key


