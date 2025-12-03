# Test the memoizer here
from nuthatch.processors import timeseries
from nuthatch import cache, set_parameter
import xarray as xr
import numpy as np
import pandas as pd
import nuthatch.memoizer
import sys

import logging
logger = logging.getLogger(__name__)

@cache(cache=False, cache_args=['name', 'species', 'stride'])
def simple_data(name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    return name



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



def test_memoizer_overflow():
    set_parameter(100000, 'remote_cache_size', location='root')
    set_parameter(1000, 'local_cache_size', location='root')

    for i in range(0, 10000):
        usage = nuthatch.memoizer.get_cache_usage(nuthatch.memoizer.remote_object_size)
        ds = simple_xarray_timeseries("2000-01-01", "2005-01-01", name=str(i), memoize=True)
        if usage + ds.nbytes > 100000:
            break
        logger.info(nuthatch.memoizer.get_cache_usage(nuthatch.memoizer.remote_object_size))

    for i in range(0, 10000):
        usage = nuthatch.memoizer.get_cache_usage(nuthatch.memoizer.local_object_size)
        ds = simple_data(name=str(i), memoize=True)
        if usage + sys.getsizeof(ds) > 1000:
            break
        logger.info(nuthatch.memoizer.get_cache_usage(nuthatch.memoizer.local_object_size))
