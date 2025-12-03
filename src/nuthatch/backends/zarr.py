from nuthatch.backend import FileBackend, register_backend
import xarray as xr
import numpy as np

import logging
logger = logging.getLogger(__name__)


def get_chunk_size(ds, size_in='MB'):
    """Get the chunk size of a dataset in MB or number of chunks.

    Args:
        ds (xr.Dataset): The dataset to get the chunk size of.
        size_in (str): The size to return the chunk size in. One of:
            'KB', 'MB', 'GB', 'TB' for kilo, mega, giga, and terabytes respectively.
    """
    if isinstance(ds, xr.DataArray):
        chunk_groups = [(dim, np.median(chunks)) for dim, chunks in ds.to_dataset().chunks.items()]
    else:
        chunk_groups = [(dim, np.median(chunks)) for dim, chunks in ds.chunks.items()]
    div = {'KB': 10**3, 'MB': 10**6, 'GB': 10**9, 'TB': 10**12}[size_in]
    chunk_sizes = [x[1] for x in chunk_groups]
    return np.prod(chunk_sizes) * 4 / div, chunk_groups


def merge_chunk_by_arg(chunking, chunk_by_arg, kwargs):
    """Merge chunking and chunking modifiers into a single chunking dict.

    Args:
        chunking (dict): The chunking to merge.
        chunk_by_arg (dict): The chunking modifiers to merge.
        kwargs (dict): The kwargs to check for chunking modifiers.
    """
    if chunk_by_arg is None:
        return chunking

    for k in chunk_by_arg:
        if k not in kwargs:
            raise ValueError(f"Chunking modifier {k} not found in kwargs.")

        if kwargs[k] in chunk_by_arg[k]:
            # If argument value in chunk_by_arg then merge the chunking
            chunk_dict = chunk_by_arg[k][kwargs[k]]
            chunking.update(chunk_dict)

    return chunking


def prune_chunking_dimensions(ds, chunking):
    """Prune the chunking dimensions to only those that exist in the dataset.

    Args:
        ds (xr.Dataset): The dataset to check for chunking dimensions.
        chunking (dict): The chunking dimensions to prune.
    """
    # Drop any dimensions that don't exist in the ds_chunks
    new_chunks = {}
    for dim in chunking:
        if dim in ds.dims:
            new_chunks[dim] = chunking[dim]

    return new_chunks


def chunking_compare(ds, chunking):
    """Compare the chunking of a dataset to a specified chunking.

    Args:
        ds (xr.Dataset): The dataset to check the chunking of.
        chunking (dict): The chunking to compare to.
    """
    # Get the chunks for the dataset
    ds_chunks = {dim: ds.chunks[dim][0] for dim in ds.chunks}
    chunking = prune_chunking_dimensions(ds, chunking)
    return ds_chunks == chunking


def drop_encoded_chunks(ds):
    """Drop the encoded chunks from a dataset."""
    if isinstance(ds, xr.DataArray):
        if 'chunks' in ds.encoding:
            del ds.encoding['chunks']
        if 'preferred_chunks' in ds.encoding:
            del ds.encoding['preferred_chunks']
    else:
        for var in ds.data_vars:
            if 'chunks' in ds[var].encoding:
                del ds[var].encoding['chunks']
            if 'preferred_chunks' in ds[var].encoding:
                del ds[var].encoding['preferred_chunks']

    for coord in ds.coords:
        if 'chunks' in ds[coord].encoding:
            del ds[coord].encoding['chunks']
        if 'preferred_chunks' in ds[coord].encoding:
            del ds[coord].encoding['preferred_chunks']

    return ds


@register_backend
class ZarrBackend(FileBackend):
    """
    Zarr backend for caching data in a zarr store.

    This backend supports xarray datasets.

    Possible backend_kwargs:
        chunking(dict): Specifies chunking if that coordinate exists. If coordinate does not exist
            the chunking specified will be dropped.
        chunk_by_arg(dict): Specifies chunking modifiers based on the passed cached arguments,
            e.g. grid resolution.  For example:
            chunk_by_arg={
                'grid': {
                    'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
                    'global1_5': {"lat": 121, "lon": 240, 'time': 1000}
                }
            }
            will modify the chunking dict values for lat, lon, and time, depending
            on the value of the 'grid' argument. If multiple cache arguments specify
            modifiers for the same chunking dimension, the last one specified will prevail.
        auto_rechunk(bool): If True will aggressively rechunk a cache on load.
        chunk_size_upper_limit_mb (int): upper limit chunk size to print warnings
        chunk_size_lower_limit_mb (int): lower limit chunk size to print warnings
    """

    backend_name = 'zarr'
    default_for_type = [xr.Dataset, xr.DataArray]

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs={}):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, extension='zarr')

        if 'chunking' in backend_kwargs and 'chunk_by_arg' in backend_kwargs:
            self.chunking = merge_chunk_by_arg(self.backend_kwargs['chunking'], self.backend_kwargs['chunk_by_arg'], args)
        else:
            self.chunking = backend_kwargs.get('chunking', 'auto')

        self.auto_rechunk = backend_kwargs.get('auto_rechunk', False)
        self.chunk_size_upper_limit_mb = backend_kwargs.get('chunk_size_upper_limit_mb', 300)
        self.chunk_size_lower_limit_mb = backend_kwargs.get('chunk_size_upper_limit_mb', 30)

    def write(self, data):
        if isinstance(data, xr.Dataset) or isinstance(data, xr.DataArray):
            data = data.persist()
            self.chunk_to_zarr(data, self.path)
            return data
        else:
            raise NotImplementedError("Zarr backend only supports caching of xarray datasets.")

    def upsert(self, data, upsert_keys=None):
        raise NotImplementedError("Zarr backend does not support upsert.")

    def read(self, engine):
        if engine == 'xarray' or engine == xr.Dataset or engine == xr.DataArray or engine is None:
            # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
            # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
            # every chunk is only 4B!
            if self.auto_rechunk:
                # If rechunk is passed then check to see if the rechunk array
                # matches chunking. If not then rechunk
                if engine == xr.DataArray:
                    ds_remote = xr.open_dataarray(self.path, engine='zarr', chunks={}, decode_timedelta=True)
                    if not isinstance(self.chunking, dict):
                        raise ValueError("If auto_rechunk is True, a chunking dict must be supplied.")
                else:
                    ds_remote = xr.open_dataset(self.path, engine='zarr', chunks={}, decode_timedelta=True)
                    if not isinstance(self.chunking, dict):
                        raise ValueError("If auto_rechunk is True, a chunking dict must be supplied.")

                # Compare the dict to the rechunk dict
                if not chunking_compare(ds_remote, self.chunking):
                    logger.info("Rechunk was passed and cached chunks do not match rechunk request. "
                          "Performing rechunking.")

                    # write to a temp cache map
                    # writing to temp cache is necessary because if you overwrite
                    # the original cache map it will write it before reading the
                    # data leading to corruption.
                    self.chunk_to_zarr(ds_remote, self.temp_path)

                    # Remove the old cache and verify files
                    if self.fs.exists(self.path):
                        self.fs.rm(self.path, recursive=True)

                    self.fs.mv(self.temp_path, self.path, recursive=True)

                    # Reopen the dataset - will use the appropriate global or local cache
                    return xr.open_dataset(self.path, engine='zarr',
                                           chunks={}, decode_timedelta=True)
                else:
                    # Requested chunks already match rechunk.
                    return xr.open_dataset(self.path, engine='zarr',
                                           chunks={}, decode_timedelta=True)
            else:
                if engine == xr.DataArray:
                    return xr.open_dataarray(self.path, engine='zarr', chunks={}, decode_timedelta=True)
                else:
                    return xr.open_dataset(self.path, engine='zarr', chunks={}, decode_timedelta=True)
        else:
            raise NotImplementedError(f"Zarr backend does not support reading zarrs to {engine} engine")


    def chunk_to_zarr(self, ds, path):
        """Write a dataset to a zarr cache map and check the chunking."""
        ds = drop_encoded_chunks(ds)

        chunking = self.chunking
        if isinstance(self.chunking, dict):
            # No need to prune if chunking is None or 'auto'
            chunking = prune_chunking_dimensions(ds, self.chunking)

        ds = ds.chunk(chunks=chunking)

        try:
            chunk_size, chunk_with_labels = get_chunk_size(ds)

            if chunk_size > self.chunk_size_upper_limit_mb or chunk_size < self.chunk_size_lower_limit_mb:
                logger.warning(f"WARNING: Chunk size is {chunk_size}MB. Target approx 100MB.")
                logger.warning(chunk_with_labels)
        except ValueError:
            logger.warning("Failed to get chunks size! Continuing with unknown chunking...")

        ds.to_zarr(store=path, mode='w')


