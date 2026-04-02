import os
import shutil
import terracotta as tc
import sqlalchemy
import xarray as xr
import rioxarray # Must import for .rio to work # noqa: F401
import numpy as np
from pyproj import CRS
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio.transform import Affine
from rasterio.warp import aligned_target, calculate_default_transform, transform
from nuthatch.backend import DatabaseBackend, FileBackend, register_backend

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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


def _get_regular_resolution(coords, dim_name):
    """Infer the resolution of a regularly spaced coordinate axis."""
    values = np.asarray(coords.values, dtype=float)
    if values.size < 2:
        raise ValueError(f"Need at least two coordinates along {dim_name} to infer resolution.")

    diffs = np.diff(values)
    resolution = float(np.abs(diffs[0]))
    if resolution == 0.0:
        raise ValueError(f"Coordinate spacing along {dim_name} must be non-zero.")
    if not np.allclose(np.abs(diffs), resolution):
        raise ValueError(f"Coordinates along {dim_name} must be regularly spaced.")
    return resolution


def _get_aligned_mercator_target(ds):
    """Build a scope-independent Web Mercator target for this raster resolution."""
    # Convert the source cell spacing to an approximate target spacing in Web Mercator meters.
    projected_x, projected_y = transform(
        "EPSG:4326",
        "EPSG:3857",
        [0.0, _get_regular_resolution(ds.x, "x")],
        [0.0, _get_regular_resolution(ds.y, "y")],
    )
    target_resolution = (
        abs(float(projected_x[1] - projected_x[0])),
        abs(float(projected_y[1] - projected_y[0])),
    )

    # Ask rasterio for a projected target grid at that resolution, then snap it to aligned pixel edges.
    target_transform, width, height = calculate_default_transform(
        "EPSG:4326",
        "EPSG:3857",
        ds.sizes["x"],
        ds.sizes["y"],
        *ds.rio.bounds(),
        resolution=target_resolution,
    )
    target_transform, width, height = aligned_target(
        target_transform,
        width,
        height,
        target_resolution,
    )
    return target_transform, width, height


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
        FileBackend.__init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs, extension='terracotta')
        DatabaseBackend.__init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs)

        if 'write_username' in self.config and 'write_password' in self.config:
            tc.update_settings(SQL_USER=self.config['write_username'], SQL_PASSWORD=self.config['write_password'])
        else:
            tc.update_settings(SQL_USER=self.config['username'], SQL_PASSWORD=self.config['password'])

        tc_url = (self.write_connection.url.drivername + "://" +
                  self.write_connection.url.host + ":" +
                  str(self.write_connection.url.port) + "/" +
                  self.write_connection.url.database)
        self.driver = tc.get_driver(tc_url)

        try:
            self.driver.get_keys()
        except sqlalchemy.exc.DatabaseError:
            # Create a metastore
            logger.info("Creating new terracotta metastore")
            self.driver.create(['key'])

        if 'override_path' in self.config:
            base_path = self.config['override_path']

            if namespace:
                self.raw_override_path = os.path.join(base_path, namespace, cache_key)
            else:
                self.raw_override_path = os.path.join(base_path, cache_key)

            self.override_path = self.raw_override_path + '.terracotta'
        else:
            self.override_path = self.path

        self.lat_dim = backend_kwargs.get('latitude_dimension', 'lat')
        self.lon_dim = backend_kwargs.get('longitude_dimension', 'lon')
        self.time_dim = backend_kwargs.get('time_dimension', 'time')
        resample = backend_kwargs.get('resampling_method', 'nearest')
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

        ds = ds.rename({self.lat_dim: 'y'})
        ds = ds.rename({self.lon_dim: 'x'})

        # Adjust coordinates
        if (ds['x'] > 180.0).any():
            # Data for terrcotta must be stored in 180 format
            lon_base_change(ds, to_base="base180", lon_dim='x')
            ds = ds.sortby(['x'])

        if self.time_dim in ds.dims:
            ds = ds.transpose(self.time_dim, 'y', 'x')
        else:
            ds = ds.transpose('y', 'x')

        # Adapt the CRS to Web Mercator.
        ds.rio.write_crs("epsg:4326", inplace=True)
        ds.rio.set_spatial_dims("x", "y", inplace=True)
        mercator_transform, width, height = _get_aligned_mercator_target(ds)
        ds = ds.rio.reproject(
            "EPSG:3857",
            transform=mercator_transform,
            shape=(height, width),
            resampling=self.resampling,
            nodata=np.nan,
        )
        ds.rio.write_crs("epsg:3857", inplace=True)

        # Insert the parameters.
        with self.driver.connect(verify=False):
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
                override_path = os.path.join(self.override_path, '_.tif')
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

            logger.info(f"Inserted {cache_key.replace('/', '_')} into the terracotta database.")

    def upsert(self, data, upsert_keys=None):
        raise NotImplementedError("Terracotta does not support upsert.")

    def read(self, engine):
        datasets_table = sqlalchemy.Table("datasets", self.driver.meta_store.sqla_metadata,
                                          autoload_with=self.driver.meta_store.sqla_engine)
        stmt = (
            datasets_table.select()
            .where(datasets_table.c['key'].like(self.cache_key.replace('/', '_') + '%'))
        )

        with self.driver.meta_store.connect() as conn:
            result = conn.execute(stmt).all()

        datasets = [row[0] for row in result]

        ret = []
        for dataset in datasets:
            ret.append({dataset: self.driver.get_metadata({'key' : dataset})})

        return ret

    def delete(self):
        # Delete the written file/folder
        self.fs.rm(self.path, recursive=True)

        datasets_table = sqlalchemy.Table("datasets", self.driver.meta_store.sqla_metadata,
                                          autoload_with=self.driver.meta_store.sqla_engine)
        stmt = (
            datasets_table.select()
            .where(datasets_table.c['key'].like(self.cache_key.replace('/', '_') + '%'))
        )


        with self.driver.meta_store.connect() as conn:
            result = conn.execute(stmt).all()

        datasets = [row[0] for row in result]
        for dataset in datasets:
            logger.info(f"Deleting datasets {datasets} from terracotta.")
            self.driver.delete({'key': dataset})
