from nuthatch import cache
import numpy as np

@cache(cache_args=['number'])
def num(number=5):
    """Test function for tabular data."""
    return np.random.randint(1000)


@cache(cache_args=['el'])
def ls(el):
    """Test with list."""
    ret = [np.random.randint(1)]
    ret.append(el)

    return ret


def test_basic():
    """Test the basic function."""
    num(10)
    ls('test')
