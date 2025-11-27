# Test the memoizer here
from nuthatch import cache
from nuthatch.processors import timeseries
import xarray as xr
import numpy as np
import pandas as pd

@timeseries()
@cache(cache=False, cache_args=['name', 'species', 'stride'])
def simple_xarray_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds


def test_memoizer():
    ds = simple_xarray_timeseries("2000-01-01", "2005-01-01", memoize=True)
    ds = simple_xarray_timeseries("2000-01-01", "2005-01-01", memoize=True)

    # Miss the memoizer
    ds = simple_xarray_timeseries("1998-01-01", "2005-01-01", memoize=True)

    assert ds['time'].min().compute() == pd.to_datetime("1998-01-01")
