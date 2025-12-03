# Test storage backends differeing from read backends
import uuid
from nuthatch import cache, set_parameter

mirror_filesystem = "gs://sheerwater-datalake/temp/" + str(uuid.uuid4())
root_filesystem = "gs://sheerwater-datalake/temp/" + str(uuid.uuid4())

def test_mirror():
    # Make a function that will be cached
    @cache(cache_args=[])
    def test_key(n):
        return n

    # Set the mirror to the root to save it in the mirror
    set_parameter(mirror_filesystem, 'filesystem', location='root')

    # Save 5 in the mirror
    d = test_key(5)

    # Now set the mirror and root back
    set_parameter(mirror_filesystem, 'filesystem', location='mirror')
    set_parameter(root_filesystem, 'filesystem', location='root')

    # Make sure we get the mirror value not the func value
    d = test_key(10)
    assert d == 5
