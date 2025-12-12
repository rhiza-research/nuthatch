# Test local caching/syncing here
from nuthatch import cache
import numpy as np


@cache(cache=True, memoize=False, cache_args=['el'])
def ls(el):
    """Test with list."""
    ret = [np.random.randint(100000)]
    ret.append(el)
    return ret


def test_local_storage():
    # Cache = False, no caching
    ds = ls('josh', cache_mode='off')
    ds2 = ls('josh', cache_mode='off')
    assert ds != ds2

    # Should store in local cache
    ds3 = ls('josh', cache_mode='local_overwrite', recompute=True)
    ds4 = ls('josh', cache_mode='local')
    assert ds3 != ds2
    assert ds3 == ds4

    # now cache a new one 
    ds5 = ls('josh', recompute=True, cache_mode='overwrite')
    # Get the current local (not synced)
    ds5b = ls('josh', cache_mode='local')
    assert ds5 != ds5b
    # Sync the local cache to the root cache
    ds5c = ls('josh', cache_mode='local_sync')
    assert ds5 == ds5c

    # Write a new local cache
    ds6 = ls('josh', recompute=True, cache_mode='local_overwrite')
    assert ds5b != ds6

    # This should hit local first and return
    ds7 = ls('josh', cache_mode='local')
    ds8 = ls('josh', cache_mode='write')
    assert ds6 == ds7
    assert ds7 != ds8
