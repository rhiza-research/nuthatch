import pytest
from nuthatch.processors import timeseries
from nuthatch import cache
import xarray as xr
import numpy as np
import pandas as pd


pytestmark = [pytest.mark.s3, pytest.mark.gcs, pytest.mark.azure]

@timeseries()
@timeseries()
def simple_bare_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds


@timeseries()
@timeseries()
@cache(cache_args=['name', 'species', 'stride'])
def simple_chained_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds



@timeseries()
@cache(cache_args=['name', 'species', 'stride'])
def simple_xarray_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds

@timeseries()
@cache(cache_args=['name', 'species', 'stride'])
def simple_kwargs_timeseries(start_time="2000-06-01", end_time="2000-07-01", name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds

@timeseries()
@cache(cache_args=['stride'])
def simple_tabular_timeseries(start_time, end_time, stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    data = {'time': times, 'obs': obs}
    df = pd.DataFrame(data)
    return df


@timeseries(timeseries='time2')
@cache(cache_args=['name', 'species', 'stride'])
def new_name_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = pd.date_range(start_time, end_time, freq='1d')
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time2', obs)}, coords={'time2': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds

def test_xarray_timeseries(cloud_storage):
    ds = simple_xarray_timeseries("2000-01-01", "2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = simple_xarray_timeseries(None, None)
    ds3 = simple_xarray_timeseries("2000-06-01", "2000-07-01")

    assert ds.equals(ds2)
    assert ds3['time'].max().values == pd.Timestamp("2000-07-01")
    assert ds3['time'].min().values == pd.Timestamp("2000-06-01")

def test_tabular_timeseries(cloud_storage):
    ds = simple_tabular_timeseries("2000-01-01", "2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = simple_tabular_timeseries(None, None)
    ds3 = simple_tabular_timeseries("2000-06-01", "2000-07-01")

    assert len(ds) == len(ds2)
    assert ds3['time'].max() == pd.Timestamp("2000-07-01")
    assert ds3['time'].min() == pd.Timestamp("2000-06-01")

def test_different_col_name(cloud_storage):
    ds = new_name_timeseries("2000-01-01", "2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = new_name_timeseries(None, None)
    ds3 = new_name_timeseries("2000-06-01", "2000-07-01")

    assert ds.equals(ds2)
    assert ds3['time2'].max().values == pd.Timestamp("2000-07-01")
    assert ds3['time2'].min().values == pd.Timestamp("2000-06-01")

def test_no_time(cloud_storage):
    @timeseries()
    @cache(cache_args=['test'])
    def no_timeseries(start_time, end_time, test):
        times = pd.date_range(start_time, end_time, freq='1d')
        obs = np.random.randint(0, 10, size=(len(times),))
        ds = xr.Dataset({'obs': ('time2', obs)}, coords={'time2': times})
        return ds

    with pytest.raises(RuntimeError):
        no_timeseries("2000-01-01", "2000-06-01", 'test')


def test_argument_validation():
    """Test that @timeseries raises ValueError if end_time arg is missing.

    No cloud_storage needed - validation error raised before cache is accessed.
    """
    @timeseries()
    @cache(cache_args=['test'])
    def bad_timeseries(start_time, test):
        return True

    with pytest.raises(ValueError):
        bad_timeseries("2000-01-01", 'test')

def test_data_validation(cloud_storage):
    @timeseries(validate_data=True)
    @cache(cache_args=['name', 'species', 'stride'])
    def simple_validate_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
        """Generate a simple timeseries dataset for testing."""
        times = pd.date_range(start_time, end_time, freq='1d')
        obs = np.random.randint(0, 10, size=(len(times),))
        ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
        ds.attrs['name'] = name
        ds.attrs['species'] = species
        return ds

    ds = simple_validate_timeseries("2000-01-01", "2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = simple_validate_timeseries("2000-01-01", "2001-01-01")
    assert ds.equals(ds2)

    # Should automatically trigger recompute and provide more data
    with pytest.raises(ValueError):
        simple_validate_timeseries("2000-01-01", "2005-01-01")


def test_data_validation_as_arg(cloud_storage):
    @timeseries()
    @cache(cache_args=['name', 'species', 'stride'])
    def simple_validate_timeseries(start_time, end_time, name='test', species='coraciidae', stride='day'):
        """Generate a simple timeseries dataset for testing."""
        times = pd.date_range(start_time, end_time, freq='1d')
        obs = np.random.randint(0, 10, size=(len(times),))
        ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
        ds.attrs['name'] = name
        ds.attrs['species'] = species
        return ds

    ds = simple_validate_timeseries("2000-01-01", "2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = simple_validate_timeseries("2000-01-01", "2001-01-01", validate_data=True)
    assert ds.equals(ds2)

    # Should automatically trigger recompute and provide more data
    with pytest.raises(ValueError):
        simple_validate_timeseries("2000-01-01", "2005-01-01", validate_data=True)

def test_memoize_post(cloud_storage):
    simple_kwargs_timeseries(start_time="2000-01-01", end_time="2001-01-01", recompute=True, cache_mode='overwrite')

    # Save into memory with smaller bounds
    ds3 = simple_kwargs_timeseries(start_time="2000-06-04", end_time="2000-06-28", memoize=True)
    ds4 = simple_kwargs_timeseries(start_time="2000-06-04", end_time="2000-06-28", memoize=True, cache_mode='off')

    assert ds3['time'].max().values == pd.Timestamp("2000-06-28")
    assert ds3['time'].min().values == pd.Timestamp("2000-06-04")
    assert ds3 == ds4


def test_kwargs(cloud_storage):
    simple_kwargs_timeseries(start_time="2000-01-01", end_time="2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = simple_kwargs_timeseries()
    ds3 = simple_kwargs_timeseries(start_time="2000-06-04", end_time="2000-06-28")

    assert ds2['time'].max().values == pd.Timestamp("2000-07-01")
    assert ds2['time'].min().values == pd.Timestamp("2000-06-01")
    assert ds3['time'].max().values == pd.Timestamp("2000-06-28")
    assert ds3['time'].min().values == pd.Timestamp("2000-06-04")

def test_chained(cloud_storage):
    simple_chained_timeseries(start_time="2000-01-01", end_time="2001-01-01", recompute=True, cache_mode='overwrite')
    ds2 = simple_chained_timeseries(start_time="2000-06-04", end_time="2000-06-28")
    assert ds2['time'].max().values == pd.Timestamp("2000-06-28")
    assert ds2['time'].min().values == pd.Timestamp("2000-06-04")

def test_bare():
    """Test bare timeseries processor without caching."""
    ds = simple_bare_timeseries(start_time="2000-01-01", end_time="2001-01-01")
    assert ds['time'].max().values == pd.Timestamp("2001-01-01")
    assert ds['time'].min().values == pd.Timestamp("2000-01-01")

