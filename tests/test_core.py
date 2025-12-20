import pytest
from nuthatch import cache
import numpy as np


@cache(cache_args=['number'], version="test")
def num(number=5):
    """Test function for tabular data."""
    return np.random.randint(100000)


@cache(cache_args=['el'])
def ls(el):
    """Test with list."""
    ret = [np.random.randint(1)]
    ret.append(el)
    return ret


@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
def test_core(cloud_storage):
    """Test the basic function."""
    data = num(10)
    data2 = num(10)
    assert data == data2

    data3 = ls('test')
    data4 = ls('test')
    assert data3 == data4

    data5 = num(10, recompute=True, cache_mode='overwrite')
    assert data5 != data

    data6 = num(11)
    assert data6 != data


@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
def test_filepath(cloud_storage):
    data = num(10, filepath_only=True)
    assert data.endswith('.pkl')


@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
def test_local_sync(cloud_storage):
    data = num(10, recompute=True, cache_mode='overwrite')

    # Should sync the data to local
    data2 = num(10, cache_local=True)

    assert data == data2

    data3 = num(10, recompute=True, cache_mode='overwrite')
    # Should resync the data to local
    data4 = num(10, cache_local=True)

    assert data2 != data3
    assert data4 == data3
