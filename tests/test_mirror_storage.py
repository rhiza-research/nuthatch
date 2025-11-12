# Test storage backends differeing from read backends
import uuid
from nuthatch import cache, config_parameter

mirror_filesystem = "gs://sheerwater-datalake/temp/" + str(uuid.uuid4())
root_filesystem = "gs://sheerwater-datalake/temp/" + str(uuid.uuid4())

def set_mirror():
    return mirror_filesystem

def set_root():
    return root_filesystem


def test_mirror():
    # Make a function that will be cached
    @cache(cache_args=[])
    def test_key(n):
        return n

    # Set the mirror to the root to save it in the mirror
    cf = config_parameter('filesystem', location='root')
    cf(set_mirror)

    # Save 5 in the mirror
    d = test_key(5)

    # Now set the mirror and root back
    cf = config_parameter('filesystem', location='mirror')
    cf(set_mirror)
    cf = config_parameter('filesystem', location='root')
    cf(set_root)

    # Make sure we get the mirror value not the func value
    d = test_key(10)
    assert d == 5
