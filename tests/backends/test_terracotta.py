from nuthatch import cache
import xarray as xr
import numpy as np
import pandas as pd
from google.cloud import secretmanager
from nuthatch.config import config_parameter


@config_parameter('password', location='root')
def postgres_write_password():
    """Get a postgres write password."""
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={"name": "projects/750045969992/secrets/postgres-write-password/versions/latest"})
    key = response.payload.data.decode("UTF-8")

    return key


@cache(cache_args=[])
def simple_latlon_array():
    """Generate a simple timeseries dataset for testing."""
    lats = np.arange(-90, 90, 0.25)
    lons = np.arange(-180, 180, 0.25)


    data_shape = (len(lats), len(lons))
    empty_data = np.ones(data_shape) * np.random.randint(1000000)

    ds = xr.Dataset(
        data_vars= {
            'data': (['lat', 'lon'], empty_data),
        },
        coords={"lat": lats, "lon": lons},
    )

    return ds


@cache(cache_args=[])
def time_latlon_array():
    """Generate a simple timeseries dataset for testing."""
    lats = np.arange(-90, 90, 0.25)
    lons = np.arange(-180, 180, 0.25)
    times = pd.date_range("2001-01-01", "2001-01-05", freq='1d')


    data_shape = (len(lats), len(lons), len(times))
    empty_data = np.ones(data_shape) * np.random.randint(1000000)

    ds = xr.Dataset(
        data_vars= {
            'data': (['lat', 'lon', 'time'], empty_data),
        },
        coords={"lat": lats, "lon": lons, "time": times},
    )

    return ds


def test_terracotta():
    # cache the array
    ds = simple_latlon_array(backend='zarr')
    # Write to terracotta
    ds = simple_latlon_array(backend='zarr', storage_backend='terracotta', cache_mode='overwrite')
    ds = simple_latlon_array(backend='terracotta')
    assert len(ds) == 1

    # cache the array
    ds = time_latlon_array(backend='zarr')
    # Verify that write?
    ds = time_latlon_array(backend='zarr', storage_backend='terracotta', cache_mode='overwrite')
    ds = time_latlon_array(backend='terracotta')
    assert len(ds) == 5
