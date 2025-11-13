# Test different namespaces here
from nuthatch import cache
import numpy as np

@cache(cache_args=['number'], version="test")
def num(number=5):
    """Test function for tabular data."""
    return np.random.randint(1000)


@cache(cache_args=['el'])
def ls(el):
    """Test with list."""
    ret = [np.random.randint(1)]
    ret.append(el)

    return ret


def test_namespace():
    """Test the basic function."""
    data = num(10, namespace="test1")
    data2 = num(10, namespace="test1")
    assert data == data2

    data3 = num(10, namespace="test2")
    assert data != data3
