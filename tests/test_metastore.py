from nuthatch.cache import Cache
from nuthatch.config import get_config
import uuid

test_config = {
    'filesystem':'gs://sheerwater-datalake/temp/',
    'filesystem_options':{'token':'google_default'}
}

def test_meta_init():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', None, None)
    assert cache.dt
    assert cache.dt.metadata()

def test_requested_backend():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)
    assert cache.get_backend() == 'basic'


def test_write_exists():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()

def test_infer_backend():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    a = 5
    cache.write(a)

    cache = Cache(local_config, "test_key", "test", {}, 'root', None, None)
    assert cache.get_backend() == 'basic'


def test_write_read():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()
    data = cache.read()

    assert data == a

def test_overwrite():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    a = 10
    cache.write(a)
    data = cache.read()
    assert data == a


def test_set_recall_null():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    cache.set_null()
    assert cache.is_null()

def test_delete():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    cache.delete()
    assert not cache.exists()

def test_delete_null():
    local_config = test_config
    local_config['filesystem'] = test_config['filesystem'] + str(uuid.uuid4())
    cache = Cache(local_config, "test_key", "test", {}, 'root', 'basic', None)

    cache.set_null()
    assert cache.is_null()

    cache.delete_null()
    assert not cache.is_null()
