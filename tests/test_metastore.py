from nuthatch.cache import Cache
from nuthatch.config import get_config

test_config = {
    'filesystem':'gs://sheerwater-datalake/temp/' + str(uuid.uuid4()),
    'filesystem_options':{'token':'google_default'}
}

def test_meta_init():
    import uuid
    cache = Cache(test_config, "test_key", "test", {}, 'root', None, None)
    assert cache.dt
    assert cache.dt.metadata()

def test_write():
    pass


def test_write_exists():
    pass


def test_infer_backend():
    pass


def test_write_read():
    pass


def test_set_recall_null():
    pass
