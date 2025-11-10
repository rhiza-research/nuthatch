from nuthatch.processors import timeseries
from nuthatch import cache
import xarray as xr
import numpy as np
import pandas as pd


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

def test_xarray_timeseries():
    ds = simple_xarray_timeseries("2000-01-01", "2001-01-01", recompute=True, force_overwrite=True)
    ds2 = simple_xarray_timeseries(None, None)
    ds3 = simple_xarray_timeseries("2000-06-01", "2000-07-01")

    assert ds.equals(ds2)
    assert ds3['time'].max().values == pd.Timestamp("2000-07-01")
    assert ds3['time'].min().values == pd.Timestamp("2000-06-01")

def test_tabular_timeseries():
    ds = simple_tabular_timeseries("2000-01-01", "2001-01-01", recompute=True, force_overwrite=True)
    ds2 = simple_tabular_timeseries(None, None)
    ds3 = simple_tabular_timeseries("2000-06-01", "2000-07-01")

    assert len(ds) == len(ds2)
    assert ds3['time'].max() == pd.Timestamp("2000-07-01")
    assert ds3['time'].min() == pd.Timestamp("2000-06-01")

def test_different_col_name():
    ds = new_name_timeseries("2000-01-01", "2001-01-01", recompute=True, force_overwrite=True)
    ds2 = new_name_timeseries(None, None)
    ds3 = new_name_timeseries("2000-06-01", "2000-07-01")

    assert ds.equals(ds2)
    assert ds3['time2'].max().values == pd.Timestamp("2000-07-01")
    assert ds3['time2'].min().values == pd.Timestamp("2000-06-01")

def test_no_time():
    @timeseries()
    @cache(cache_args=['test'])
    def no_timeseries(start_time, end_time, test):
        times = pd.date_range(start_time, end_time, freq='1d')
        obs = np.random.randint(0, 10, size=(len(times),))
        ds = xr.Dataset({'obs': ('time2', obs)}, coords={'time2': times})
        return ds

    try:
        no_timeseries("2000-01-01", "2000-06-01", 'test')
        assert False
    except RuntimeError:
        assert True


def test_argument_validation():
    @timeseries()
    @cache(cache_args=['test'])
    def bad_timeseries(start_time, test):
        return True

    try:
        bad_timeseries("2000-01-01", 'test')
        assert False
    except ValueError:
        assert True

def test_data_validation():
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

    ds = simple_validate_timeseries("2000-01-01", "2001-01-01", recompute=True, force_overwrite=True)
    ds2 = simple_validate_timeseries("2000-01-01", "2001-01-01")
    assert ds.equals(ds2)

    # Should automatically trigger reocmpute and provide more data
    try:
        ds3 = simple_validate_timeseries("2000-01-01", "2005-01-01") # noqa: F841
        assert False
    except ValueError:
        assert True


def test_data_validation_as_arg():
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

    ds = simple_validate_timeseries("2000-01-01", "2001-01-01", recompute=True, force_overwrite=True)
    ds2 = simple_validate_timeseries("2000-01-01", "2001-01-01", validate_data=True)
    assert ds.equals(ds2)

    # Should automatically trigger reocmpute and provide more data
    try:
        ds3 = simple_validate_timeseries("2000-01-01", "2005-01-01", validate_data=True) # noqa: F841
        assert False
    except ValueError:
        assert True
