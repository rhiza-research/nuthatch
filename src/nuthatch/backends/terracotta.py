import os
import shutil
from typing import Tuple
import terracotta as tc
import sqlalchemy
import xarray as xr
import numpy as np
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from nuthatch.backend import DatabaseBackend, FileBackend, register_backend

import logging
logger = logging.getLogger(__name__)

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
            logger.info("Longitude already in base 180 format.")
            return ds
        lons = base360_to_base180(ds[lon_dim].values)
    elif to_base == "base360":
        if (ds[lon_dim] > 180.0).any():
            logger.info("Longitude already in base 360 format.")
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

    All terracotta files are stored in a folder and named based on the time dimension (or _.tif) if there is not time dimension
    (i.e. filesystem/cache_key.terracotta/_.tif, or filesyste/cache_key.terracotta/1.tif)

    Additional configuration parameters:
        override_path (str): The base path to use when registering a tif with terracotta. Defaults to `filesystem`.
    """

    backend_name = 'terracotta'

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs={}):
        # This calls both inits right?
        DatabaseBackend.__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)
        FileBackend.__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, extension='terracotta')

        tc.update_settings(SQL_USER=self.config['write_username'], SQL_PASSWORD=self.config['write_password'])
        self.driver = tc.get_driver(self.write_uri)

        try:
            self.driver.get_keys()
        except sqlalchemy.exc.DatabaseError:
            # Create a metastore
            logger.info("Creating new terracotta metastore")
            self.driver.create(['key'])

        if 'override_path' in backend_kwargs:
            base_path = backend_kwargs['override_path']

            if namespace:
                self.raw_override_path = os.path.join(base_path, namespace, cache_key)
            else:
                self.raw_override_path = os.path.join(base_path, cache_key)

            self.override_path = self.raw_override_path + '.terracotta'

        self.lat_dim = backend_kwargs.get('latitude_dimension', 'lat')
        self.lon_dim = backend_kwargs.get('longitude_dimension', 'lon')
        self.time_dim = backend_kwargs.get('time_dimension', 'time')
        resample = backend_kwargs.get('resampling', 'nearest')
        if resample == 'nearest':
            self.resampling = Resampling.nearest
        elif resample == 'average':
            self.resampling = Resampling.average
        elif resample == 'bilinear':
            self.resampling = Resampling.bilinear
        elif resample == 'cubic':
            self.resampling = Resampling.cubic
        else:
            raise RuntimeError("Resampling must be set to one of `nearest`, `average`, `bilinear`, or `cubic`.")

    def write(self, ds, upsert=False, primary_keys=None):

        if not isinstance(ds, xr.Dataset):
            raise NotImplementedError("Terracotta backend only supports xarray datasets")

        # Check to make sure this is geospatial data
        if len(ds.dims) == 2:
            if self.lat_dim not in ds.dims or self.lon_dim not in ds.dims:
                raise RuntimeError("""Did not find latitude and longitude dimension in dataset.
                                   If dims are not named `lat` and `lon` set the dimension names in backend_kwargs""")
        elif len(ds.dims) == 3:
            if self.lat_dim not in ds.dims or self.lon_dim not in ds.dims or self.time_dim not in ds.dims:
                raise RuntimeError("""Did not find latitude, longitude, and time dimension in dataset.
                                   If dims are not named `lat`, `lon`, and `time` set the dimension names in backend_kwargs""")
        else:
            raise RuntimeError("Terracotta backend only supports 2D (lat/lon) and 3D (lat/lon/time) datasets.")

        ds = ds.rename(self.lat_dim, 'y')
        ds = ds.rename(self.lon_dim, 'x')

        # Adjust coordinates
        if (ds['x'] > 180.0).any():
            # Data for terrcotta must be stored in 180 format
            lon_base_change(ds, to_base="base180", lon_dim='x')
            ds = ds.sortby(['x'])

        # Adapt the CRS
        ds.rio.write_crs("epsg:4326", inplace=True)
        ds = ds.rio.reproject('EPSG:3857', resampling=self.resampling, nodata=np.nan)
        ds.rio.write_crs("epsg:3857", inplace=True)

        # Insert the parameters.
        with self.driver.connect():
            if self.time_dim in ds.dims:
                for t in ds.time:
                    # Select just this time and squeeze the dimension
                    sub_ds = ds.sel(time=t)
                    sub_ds = sub_ds.reset_coords('time', drop=True)

                    # Assume the cache_key is now a folder and make sub tifs under it
                    sub_cache_key = self.cache_key + '_' + str(t.values)
                    sub_path = os.path.join(self.path, str(t.values) + '.tif')
                    sub_override_path = os.path.join(self.override_path,  str(t.values) + '.tif')

                    self.write_individual_raster(self.driver, sub_ds, sub_path, sub_cache_key, sub_override_path)
            else:
                path = os.path.join(self.path, '_.tif')
                override_path = os.path.join(self.override_path, '_.time')
                self.write_individual_raster(self.driver, ds, path, self.cache_key, override_path)

        return ds

    def write_individual_raster(self, driver, ds, path, cache_key, override_path):
        # Write the raster
        with MemoryFile() as mem_dst:
            ds.rio.to_raster(mem_dst.name, driver="COG")

            with self.fs.open(path, 'wb') as f_out:
                shutil.copyfileobj(mem_dst, f_out)

            driver.insert({'key': cache_key.replace('/', '_')}, mem_dst,
                         override_path=override_path, skip_metadata=False)

            logger.debug(f"Inserted {cache_key.replace('/', '_')} into the terracotta database.")

    def upsert(self, data, upsert_keys=None):
        raise NotImplementedError("Terracotta does not support upsert.")

    def read(self, engine):
        raise NotImplementedError("Cannot read from the terracotta backend.")

    def delete(self):
        # Delete the written file/folder
        self.fs.rm(self.path, recursive=True)

        datasets_table = sqlalchemy.Table(
            "datasets", self.driver.meta_store.sqla_metadata, autoload_with=self.driver.meta_store.sqla_engine
        )
        stmt = (
            datasets_table.select()
            .where(datasets_table.c['key'].like(self.cache_key.replace('/', '_') + '%'))
        )


        with self.driver.meta_store.connect() as conn:
            result = conn.execute(stmt).all()

        def keytuple(row: sqlalchemy.engine.row.Row) -> Tuple[str, ...]:
            return tuple(getattr(row, key) for key in self.key_names)

        datasets = {keytuple(row): row.path for row in result}

        logger.info(f"Deleting datasets {datasets} from terracotta.")
        self.driver.delete(datasets)

