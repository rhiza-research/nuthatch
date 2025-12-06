from nuthatch.cache import Cache
import uuid

test_config = {
    'filesystem':'gs://sheerwater-datalake/temp/',
    'basic': {
        'filesystem':'gs://sheerwater-datalake/temp/',
    }
}

def test_meta_init():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, None, {})
    assert cache.metastore.table_path is not None

def test_requested_backend():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)
    assert cache.get_backend() == 'basic'


def test_write_exists():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()

def test_infer_backend():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)

    cache = Cache(local_config, "test_key", "test", None, {},  None, None)
    assert cache.get_backend() == 'basic'


def test_write_read():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)

    assert cache.exists()
    data = cache.read()

    assert data == a

def test_overwrite():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)

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
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)

    cache.set_null()
    assert cache.is_null()

def test_delete():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {}, 'basic', None)

    a = 5
    cache.write(a)
    data = cache.read()
    assert data == a

    cache.delete()
    assert not cache.exists()

def test_delete_null():
    local_config = test_config
    u = str(uuid.uuid4())
    local_config['filesystem'] = test_config['filesystem'] + u
    local_config['basic']['filesystem'] = test_config['filesystem'] + u
    cache = Cache(local_config, "test_key", "test", None, {},  'basic', None)

    cache.set_null()
    assert cache.is_null()

    cache.delete_null()
    assert not cache.is_null()
