from nuthatch import cache
from nuthatch.nuthatch import check_if_nested_fn
from nuthatch.processors import timeseries
import numpy as np
import pandas as pd
import xarray as xr

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


def test_core():
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

def test_filepath():
    data = num(10, filepath_only=True)
    assert data.endswith('.pkl')


def test_check_if_nested_fn():
    """Test check_if_nested_fn function."""
    # Test when called directly (not nested) - should return False
    assert check_if_nested_fn() == False  # noqa: E712

    @cache(cache_args=[])
    def outer_func():
        # When called from a top-level cached function, should return False
        return check_if_nested_fn()

    @cache(cache_args=[])
    def inner_func():
        # When called from within a nested cached function, should return True
        return check_if_nested_fn()

    @cache(cache_args=[])
    def nested_outer_func():
        # This calls inner_func, which should detect nesting
        return inner_func()

    # Test top-level cached function - should return False
    result = outer_func(cache_mode='off')
    assert result == False  # noqa: E712

    # Test nested cached function - should return True
    result = nested_outer_func(cache_mode='off')
    assert result == True  # noqa: E712

    # Test with processors
    @cache(cache_args=[])
    def processor_inner_func():
        # When called from within a cached function with processor, should return True
        return check_if_nested_fn()

    @timeseries()
    @cache(cache_args=[])
    def processor_outer_func(start_time="2021-01-01", end_time="2021-01-05"):
        # This calls processor_inner_func, which should detect nesting (processor wrapper)
        is_nested = processor_inner_func()
        # Return a proper timeseries dataset
        times = pd.date_range(start_time, end_time, freq='1d')
        obs = np.random.randint(0, 10, size=(len(times),))
        ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
        # Store the nested check result as an attribute for verification
        ds.attrs['is_nested'] = is_nested
        return ds

    # Test nested cached function with processor wrapper - should return True
    result = processor_outer_func(cache_mode='off')
    assert result.attrs['is_nested'] == True  # noqa: E712
