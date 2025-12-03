from nuthatch import cache
import xarray as xr
import numpy as np
import pandas as pd

@cache(cache_args=['name', 'species', 'stride'])
def simple_array(start_time="2021-01-01", end_time="2021-02-01", name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds

@cache(cache_args=['name', 'species', 'stride'],
       engine=xr.DataArray)
def simple_darray(start_time="2021-01-01", end_time="2021-02-01", name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds['obs']


@cache(cache_args=['name', 'species', 'stride'],
       backend_kwargs = {
           'chunking': {
                'time': 5
            },
           'chunk_by_arg': {
                'name': {
                    'noti': {'time': 2}
                }
            }
       })
def chunked_array(start_time="2021-01-01", end_time="2021-02-01", name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds


def test_zarr():
    """Test the tabular function."""
    data = simple_array(name='josh', recompute=True, force_overwrite=True)
    data2 = simple_array(name='josh')
    assert data.equals(data2)

    data3 = simple_array(name='josh', recompute=True, force_overwrite=True)
    data4 = simple_array(name='josh')
    assert not data.equals(data3)
    assert data3.equals(data4)

    data = simple_darray(name='josh', recompute=True, force_overwrite=True)
    data2 = simple_darray(name='josh')
    assert data.equals(data2)

    data3 = simple_darray(name='josh', recompute=True, force_overwrite=True)
    data4 = simple_darray(name='josh')
    assert not data.equals(data3)
    assert data3.equals(data4)


def test_chunking():
    chunked_array(name='josh', recompute=True, force_overwrite=True)
    data2 = chunked_array(name='josh')
    assert data2.chunks['time'][0] == 5

    chunked_array(name='noti', recompute=True, force_overwrite=True)
    data3 = chunked_array(name='noti')
    assert data3.chunks['time'][0] == 2
