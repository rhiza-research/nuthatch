# Test local caching/syncing here
from nuthatch import cache
import numpy as np

@cache(cache=False, memoize=False, cache_args=['el'])
def ls(el):
    """Test with list."""
    ret = [np.random.randint(1000)]
    ret.append(el)
    return ret

def test_local_storage():
    # Cache = False, no caching
    ds = ls('josh')
    ds2 = ls('josh')
    assert ds != ds2

    # Should store in local cache
    ds3 = ls('josh', cache_mode='local_overwrite', recompute=True)
    ds4 = ls('josh', cache_mode='local')
    assert ds3 != ds2
    assert ds3 == ds4

    # now cache a new one and make sure it copies to local
    ds5 = ls('josh', recompute=True, cache_mode='overwrite')

    # This should hit local first and return
    ds6 = ls('josh', cache_mode='local')
    assert ds5 != ds6

    # This should hit local first and return
    ds7 = ls('josh', recompute=True, cache_mode='local_overwrite')
    ds8 = ls('josh', cache_mode='local')
    assert ds5 == ds7
    assert ds5 == ds8
