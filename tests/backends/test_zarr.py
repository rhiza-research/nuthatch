from nuthatch import cache
from nuthatch.backends.zarr import is_single_chunk
import xarray as xr
import numpy as np
import pandas as pd
import logging
from nuthatch.backends.zarr import get_chunk_size

from dask.distributed import Client
import dask.array as da

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
       backend_kwargs={
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

@cache(cache_args=[],
       backend_kwargs={
           'chunking': {
               'time': 365,
               'lat': 300,
               'lon': 300
           },
})
def large_chunked_array(start_time="2000-01-01", end_time="2004-01-01"):
    """Generate a simple timeseries dataset for testing."""

    times = pd.date_range(start_time, end_time, freq='1d')
    lons = np.arange(-180, 180, 0.2)
    lats = np.arange(-90, 90, 0.2)
    data = da.random.normal(0, 1, size=(len(times), len(lats), len(lons)), chunks=(365, 300, 300))

    ds = xr.Dataset(
        data_vars={
            "obs": (("time", "lat", "lon"), data),
        },
        coords={
            "lat": lats,
            "lon": lons,
            "time": times,
        },
    )

    return ds

@cache(cache=False)
def nested_large_chunk():
    return large_chunked_array()


def test_zarr():
    """Test the tabular function."""
    data = simple_array(name='josh', recompute=True, cache_mode='overwrite')
    data2 = simple_array(name='josh')
    assert data.equals(data2)

    data3 = simple_array(name='josh', recompute=True, cache_mode='overwrite')
    data4 = simple_array(name='josh')
    assert not data.equals(data3)
    assert data3.equals(data4)

    data = simple_darray(name='josh', recompute=True, cache_mode='overwrite')
    data2 = simple_darray(name='josh')
    assert data.equals(data2)

    data3 = simple_darray(name='josh', recompute=True, cache_mode='overwrite')
    data4 = simple_darray(name='josh')
    assert not data.equals(data3)
    assert data3.equals(data4)


def test_chunking():
    chunked_array(name='josh', recompute=True, cache_mode='overwrite')
    data2 = chunked_array(name='josh')
    assert data2.chunks['time'][0] == 5

    chunked_array(name='noti', recompute=True, cache_mode='overwrite')
    data3 = chunked_array(name='noti')
    assert data3.chunks['time'][0] == 2


def test_is_single_chunk():
    """Test is_single_chunk function."""
    # Single chunk dataset
    times = pd.date_range("2021-01-01", "2021-01-10", freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds_single = xr.Dataset({'obs': ('time', obs)}, coords={'time': times}).chunk({'time': 10})
    assert is_single_chunk(ds_single) == True  # noqa: E712

    # Multi-chunk dataset
    ds_multi = xr.Dataset({'obs': ('time', obs)}, coords={'time': times}).chunk({'time': 5})
    assert is_single_chunk(ds_multi) == False  # noqa: E712

    # Single chunk DataArray
    da_single = xr.DataArray(obs, dims='time', coords={'time': times}).chunk({'time': 10})
    assert is_single_chunk(da_single) == True  # noqa: E712

    # Multi-chunk DataArray
    da_multi = xr.DataArray(obs, dims='time', coords={'time': times}).chunk({'time': 5})
    assert is_single_chunk(da_multi) == False  # noqa: E712

    # None case
    assert is_single_chunk(None) == True  # noqa: E712


@cache(cache_args=['name'])
def single_chunk_array(name='test'):
    """Generate a small dataset that will be in a single chunk."""
    times = pd.date_range("2021-01-01", "2021-01-10", freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    return ds


def test_single_chunk_no_warning(caplog):
    """Test that single chunk datasets don't trigger warnings when below lower limit."""
    with caplog.at_level(logging.WARNING):
        single_chunk_array(name='single_chunk_test', recompute=True, cache_mode='overwrite')

    # Filter for chunk size warnings
    warning_messages = [record.message for record in caplog.records
                        if 'zarr chunk size' in record.message]

    # Should not have any chunk size warnings since it's a single chunk
    assert len(warning_messages) == 0, f"Expected no warnings for single chunk, but got: {warning_messages}"

    # Verify the data can be read back
    data = single_chunk_array(name='single_chunk_test')
    assert 'obs' in data.data_vars
    assert len(data.time) == 10


def test_read_chunk_resize():
    # Cache the large array
    Client()
    ds = large_chunked_array()
    assert ds.chunksizes['time'][0] == 365
    assert ds.chunksizes['lat'][0] == 300
    assert ds.chunksizes['lon'][0] == 300

    ds = large_chunked_array(backend_kwargs={'target_read_chunk_size_mb': 300})
    size, _  = get_chunk_size(ds)
    assert size > 200 and size < 400

    ds = large_chunked_array(backend_kwargs={'target_read_chunk_size_mb': 1000})
    size, _  = get_chunk_size(ds)
    assert size > 500 and size < 1000

    ds = large_chunked_array(backend_kwargs={'target_read_chunk_size_mb': 3000})
    size, _  = get_chunk_size(ds)
    assert size > 1500 and size < 4000


def test_backend_passthrough():
    ds = nested_large_chunk(backend_kwargs={'target_read_chunk_size_mb': 3000})
    size, _  = get_chunk_size(ds)
    assert size > 1500 and size < 4000
