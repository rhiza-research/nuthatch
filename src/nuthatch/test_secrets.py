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

#@config_parameter('filesystem_options', location='root', secret=True)
#def cloudflare_private_options():
#    """Get a postgres write password."""
#    from google.cloud import secretmanager
#
#    client = secretmanager.SecretManagerServiceClient()
#
#    response = client.access_secret_version(
#        request={"name": "projects/750045969992/secrets/sheerwater-public-key/versions/latest"})
#    key = response.payload.data.decode("UTF-8")
#
#    response = client.access_secret_version(
#        request={"name": "projects/750045969992/secrets/sheerwater-public-secret/versions/latest"})
#    secret = response.payload.data.decode("UTF-8")
#
#    return {
#        's3_additional_kwargs': {
#            'ACL': 'private'
#        },
#        'client_kwargs': {
#            'endpoint_url': 'https://06d576014bb83fe1b43a35bd8eeb74c6.r2.cloudflarestorage.com/'
#        }
#    }


