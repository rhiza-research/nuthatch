from nuthatch.cache import Cache

from nuthatch.config import config_parameter
from nuthatch.config import get_config
from google.cloud import secretmanager

@config_parameter('password', location='root')
def postgres_write_password():
    """Get a postgres write password."""
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/sheerwater-postgres-write-password/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key

local_config = get_config(location='root', requested_parameters=Cache.config_parameters)
local_config['metadata_location'] = 'database'

def test_meta_init():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', None, {})
    assert cache.metastore.db_table is not None

def test_requested_backend():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)
    assert cache.get_backend() == 'basic'


def test_write_exists():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()

def test_infer_backend():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    a = 5
    cache.write(a)

    cache = Cache(local_config, "test_key", "test", None, {}, 'root', None, None)
    assert cache.get_backend() == 'basic'


def test_write_read():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()
    data = cache.read()

    assert data == a

def test_overwrite():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    a = 10
    cache.write(a)
    data = cache.read()
    assert data == a


def test_set_recall_null():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    cache.set_null()
    assert cache.is_null()

def test_delete():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    cache.delete()
    assert not cache.exists()

def test_delete_null():
    cache = Cache(local_config, "test_key", "test", None, {}, 'root', 'basic', None)

    cache.set_null()
    assert cache.is_null()

    cache.delete_null()
    assert not cache.is_null()
