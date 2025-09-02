from nuthatch.backend import DatabaseBackend, FileBackend, register_backend
import shutil
from pathlib import Path
import terracotta as tc
import sqlalchemy
import xarray as xr
import numpy as np
from rasterio.io import MemoryFile
from rasterio.enums import Resampling

def base360_to_base180(lons):
    """Converts a list of longitudes from base 360 to base 180.

    Args:
        lons (list, float): A list of longitudes, or a single longitude
    """
    if not isinstance(lons, np.ndarray) and not isinstance(lons, list):
        lons = [lons]
    val = [x - 360.0 if x >= 180.0 else x for x in lons]
    if len(val) == 1:
        return val[0]
    return np.array(val)


def base180_to_base360(lons):
    """Converts a list of longitudes from base 180 to base 360.

    Args:
        lons (list, float): A list of longitudes, or a single longitude
    """
    if not isinstance(lons, np.ndarray) and not isinstance(lons, list):
        lons = [lons]
    val = [x + 360.0 if x < 0.0 else x for x in lons]
    if len(val) == 1:
        return val[0]
    return np.array(val)



def is_wrapped(lons):
    """Check if the longitudes are wrapped.

    Works for both base180 and base360 longitudes. Requires that
    longitudes are in increasing order, outside of a wrap point.
    """
    wraps = (np.diff(lons) < 0.0).sum()
    if wraps > 1:
        raise ValueError("Only one wrapping discontinuity allowed.")
    elif wraps == 1:
        return True
    return False


def lon_base_change(ds, to_base="base180", lon_dim='lon'):
    """Change the base of the dataset from base 360 to base 180 or vice versa.

    Args:
        ds (xr.Dataset): Dataset to change.
        to_base (str): The base to change to. One of:
            - base180
            - base360
        lon_dim (str): The longitude column name.
    """
    if to_base == "base180":
        if (ds[lon_dim] < 0.0).any():
            print("Longitude already in base 180 format.")
            return ds
        lons = base360_to_base180(ds[lon_dim].values)
    elif to_base == "base360":
        if (ds[lon_dim] > 180.0).any():
            print("Longitude already in base 360 format.")
            return ds
        lons = base180_to_base360(ds[lon_dim].values)
    else:
        raise ValueError(f"Invalid base {to_base}.")

    # Check if original data is wrapped
    wrapped = is_wrapped(ds.lon.values)

    # Then assign new coordinates
    ds = ds.assign_coords({lon_dim: lons})

    # Sort the lons after conversion, unless the slice
    # you're considering wraps around the meridian
    # in the resultant base.
    if not wrapped:
        ds = ds.sortby('lon')
    return ds


@register_backend
class TerracottaBackend(DatabaseBackend, FileBackend):
    """
    Terracotta backend for caching geospatial data in a terracotta database.
    
    This backend supports xarray datasets.
    """

    backend_name = 'terracotta'
    config_parameters = DatabaseBackend.config_parameters + FileBackend.config_parameters + ['override_path']

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        # This calls both inits right?
        DatabaseBackend.__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)
        FileBackend.__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, 'tif')

        tc.update_settings(SQL_USER=self.config['write_username'], SQL_PASSWORD=self.config['write_password'])
        self.driver = tc.get_driver(self.write_uri)

        try:
            self.driver.get_keys()
        except sqlalchemy.exc.DatabaseError:
            # Create a metastore
            print("Creating new terracotta metastore")
            self.driver.create(['key'])

        if 'override_path' in backend_kwargs:
            base_path = Path(backend_kwargs['override_path'])

            if namespace:
                self.raw_override_path = base_path.joinpath(namespace, cache_key)
            else:
                self.raw_override_path = base_path.joinpath(cache_key)

            self.override_path = self.raw_override_path + '.tif'

    def write(self, ds, upsert=False, primary_keys=None):

        if not isinstance(ds, xr.Dataset):
            raise NotImplementedError("Terracotta backend only supports xarray datasets")

        # Check to make sure this is geospatial data
        lats = ['lat', 'y', 'latitude']
        lons = ['lon', 'x', 'longitude']
        if len(ds.dims) != 2:
            if len(ds.dims) != 3 or 'time' not in ds.dims:
                raise RuntimeError("Can only store two dimensional geospatial data to terracotta")

        foundx = False
        foundy = False
        for y in lats:
            if y in ds.dims:
                ds = ds.rename({y: 'y'})
                foundy = True
        for x in lons:
            if x in ds.dims:
                ds = ds.rename({x: 'x'})
                foundx = True

        if not foundx or not foundy:
            raise RuntimeError("Can only store two or three dimensional (with time) geospatial data to terracotta")

        # Adjust coordinates
        if (ds['x'] > 180.0).any():
            lon_base_change(ds, lon_dim='x')
            ds = ds.sortby(['x'])

        # Adapt the CRS
        ds.rio.write_crs("epsg:4326", inplace=True)
        ds = ds.rio.reproject('EPSG:3857', resampling=Resampling.nearest, nodata=np.nan)
        ds.rio.write_crs("epsg:3857", inplace=True)

        # Insert the parameters.
        with self.driver.connect():
            if 'time' in ds.dims:
                for t in ds.time:
                    # Select just this time and squeeze the dimension
                    sub_ds = ds.sel(time=t)
                    sub_ds = sub_ds.reset_coords('time', drop=True)

                    # add the time to the cache_key
                    sub_cache_key = self.cache_key + '_' + str(t.values)
                    sub_path = self.raw_cache_path + '_' + str(t.values) + '.tif'
                    sub_override_path = self.raw_override_path + '_' + str(t.values) + '.tif'

                    self.write_individual_raster(self.driver, sub_ds, sub_path, sub_cache_key, sub_override_path)
            else:
                self.write_individual_raster(self.driver, ds, self.path, self.cache_key, self.override_path)

            pass

    def write_individual_raster(self, driver, ds, path, cache_key, override_path):
        # Write the raster
        with MemoryFile() as mem_dst:
            ds.rio.to_raster(mem_dst.name, driver="COG")

            with self.fs.open(path, 'wb') as f_out:
                shutil.copyfileobj(mem_dst, f_out)

            driver.insert({'key': cache_key.replace('/', '_')}, mem_dst,
                         override_path=override_path, skip_metadata=False)

            print(f"Inserted {cache_key.replace('/', '_')} into the terracotta database.")

    def read(engine):
        raise NotImplementedError("Cannot read from the terracotta backend.")

    def delete():
        raise NotImplementedError("Cannot delete from the terracotta backend.")
