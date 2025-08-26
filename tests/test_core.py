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


def test_core():
    """Test the basic function."""
    data = num(10)
    data2 = num(10)
    assert data == data2


    data3 = ls('test')
    data4 = ls('test')
    assert data3 == data4

    data5 = num(10, recompute=True, force_overwrite=True)
    assert data5 != data

    data6 = num(11)
    assert data6 != data
