import pytest
from nuthatch.cache import Cache
from nuthatch.config import NuthatchConfig
import uuid


@pytest.fixture
def cache_config():
    """Get the location-specific config for Cache (uses test config provider from cloud_provider fixture)."""
    config = NuthatchConfig(wrapped_module='tests')
    root = config['root']
    # Build location-specific config like the original test_config structure
    u = str(uuid.uuid4())
    location_config = {
        'filesystem': root['filesystem'] + '/' + u,
        'basic': {
            'filesystem': root['filesystem'] + '/' + u,
        }
    }
    # Copy filesystem_options if present
    if 'filesystem_options' in root:
        location_config['filesystem_options'] = root['filesystem_options']
        location_config['basic']['filesystem_options'] = root['filesystem_options']
    return location_config


def test_meta_init(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, None, {})
    assert cache.metastore.table_path is not None


def test_requested_backend(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)
    assert cache.get_backend() == 'basic'


def test_write_exists(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()


def test_infer_backend(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)

    cache = Cache(cache_config, "test_key", "test", None, {}, None, None)
    assert cache.get_backend() == 'basic'


def test_write_read(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()
    data = cache.read()

    assert data == a


def test_overwrite(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    a = 10
    cache.write(a)
    data = cache.read()
    assert data == a


def test_set_recall_null(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    cache.set_null()
    assert cache.is_null()


def test_delete(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    cache.delete()
    assert not cache.exists()


def test_delete_null(cache_config):
    cache = Cache(cache_config, "test_key", "test", None, {}, 'basic', None)

    cache.set_null()
    assert cache.is_null()

    cache.delete_null()
    assert not cache.is_null()
