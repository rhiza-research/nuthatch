from abc import ABC, abstractmethod
import terracotta as tc
import sqlalchemy
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from sheerwater_benchmarking.utils.data_utils import lon_base_change
from sheerwater_benchmarking.utils.secrets import postgres_write_password

class TerracottaBackend(CacheableBackend):

    def __init__(self, cache_key, filesystem_like, args, backend_kwargs):
        self.cache_key = cache_key
        self.fs = filesystem_like
        self.backend_kwargs = backend_kwargs
        self.args = args

    def write(ds):
        """Write geospatial array to terracotta.

        Args:
            ds (xr.Dataset): Dataset which holds raster
        """
        cache_key = self.cache_key

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

        def write_individual_raster(driver, bucket, ds, cache_key):
            # Write the raster
            with MemoryFile() as mem_dst:
                ds.rio.to_raster(mem_dst.name, driver="COG")

                blob = bucket.blob(f'rasters/{cache_key}.tif')
                blob.upload_from_file(mem_dst)

                driver.insert({'key': cache_key.replace('/', '_')}, mem_dst,
                              override_path=f'/mnt/sheerwater-datalake/{cache_key}.tif', skip_metadata=False)

                print(f"Inserted {cache_key.replace('/', '_')} into the terracotta database.")

        storage_client = storage.Client()
        bucket = storage_client.bucket("sheerwater-datalake")

        # Register with terracotta
        tc.update_settings(SQL_USER="write", SQL_PASSWORD=postgres_write_password())
        if not hasattr(write_to_terracotta, 'driver'):
            driver = tc.get_driver("postgresql://postgres.sheerwater.rhizaresearch.org:5432/terracotta")
            write_to_terracotta.driver = driver
        else:
            driver = write_to_terracotta.driver

        try:
            driver.get_keys()
        except sqlalchemy.exc.DatabaseError:
            # Create a metastore
            print("Creating new terracotta metastore")
            driver.create(['key'])

        # Insert the parameters.
        with driver.connect():
            if 'time' in ds.dims:
                for t in ds.time:
                    # Select just this time and squeeze the dimension
                    sub_ds = ds.sel(time=t)
                    sub_ds = sub_ds.reset_coords('time', drop=True)

                    # add the time to the cache_key
                    sub_cache_key = cache_key + '_' + str(t.values)

                    write_individual_raster(driver, bucket, sub_ds, sub_cache_key)
            else:
                write_individual_raster(driver, bucket, ds, cache_key)

            pass

    def read(engine):
        raise NotImplementedError("Cannot read from the terracotta backend.")

    def delete():
        raise NotImplementedError("Cannot delete from the terracotta backend.")

    def delete_null():
        raise NotImplementedError("Terracotta does not support caching nulls.")

    @abstractmethod
    def exists():
        pass

    def null():
        raise NotImplementedError("Terracotta does not support caching nulls.")

    def get_file_path():
        raise NotImplementedError("Terracotta does not support returning file paths.")
