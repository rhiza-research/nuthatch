from nuthatch.backend import FileBackend, register_backend
import xarray as xr

CHUNK_SIZE_UPPER_LIMIT_MB = 300
CHUNK_SIZE_LOWER_LIMIT_MB = 30


def get_chunk_size(ds, size_in='MB'):
    """Get the chunk size of a dataset in MB or number of chunks.

    Args:
        ds (xr.Dataset): The dataset to get the chunk size of.
        size_in (str): The size to return the chunk size in. One of:
            'KB', 'MB', 'GB', 'TB' for kilo, mega, giga, and terabytes respectively.
    """
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
    # Get the chunks for the dataset
    ds_chunks = {dim: ds.chunks[dim][0] for dim in ds.chunks}

    # Drop any dimensions that don't exist in the ds_chunks
    dims_to_drop = []
    for dim in chunking:
        if dim not in ds_chunks:
            dims_to_drop.append(dim)

    for dim in dims_to_drop:
        del chunking[dim]

    return chunking


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
    """

    backend_name = 'zarr'
    default_for_type = xr.DataSet

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, 'zarr')

        if 'chunking' in backend_kwargs and 'chunk_by_arg' in backend_kwargs:
            self.chunking = merge_chunk_by_arg(self.backend_kwargs['chunking'], self.backend_kwargs['chunk_by_arg'], args)
        elif 'chunking' in backend_kwargs:
            self.chunking = self.backend_kwargs['chunking']
        else:
            self.chunking = 'auto'

        if 'auto_rechunk' in self.backend_kwargs and self.backend_kwargs['auto_rechunk']:
            self.auto_rechunk = True
        else:
            self.auto_rechunk = False

    def write(self, data, upsert=False, primary_keys=None):
        if upsert:
            raise NotImplementedError("Zarr backend does not support upsert.")

        if isinstance(data, xr.Dataset):
            chunk_to_zarr(data, self.path)
        else:
            raise NotImplementedError("Zarr backend only supports caching of xarray datasets.")

    def read(engine):
        if engine == 'xarray' or engine == xr.Dataset or engine == None:
            # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
            # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
            # every chunk is only 4B!
            if self.auto_rechunk:
                # If rechunk is passed then check to see if the rechunk array
                # matches chunking. If not then rechunk
                ds_remote = xr.open_dataset(self.path, engine='zarr', chunks={}, decode_timedelta=True)
                if not isinstance(self.chunking, dict):
                    raise ValueError("If auto_rechunk is True, a chunking dict must be supplied.")

                # Compare the dict to the rechunk dict
                if not chunking_compare(ds_remote, self.chunking):
                    print("Rechunk was passed and cached chunks do not match rechunk request. "
                          "Performing rechunking.")

                    # write to a temp cache map
                    # writing to temp cache is necessary because if you overwrite
                    # the original cache map it will write it before reading the
                    # data leading to corruption.
                    chunk_to_zarr(ds_remote, self.temp_path)

                    # Remove the old cache and verify files
                    if self.fs.exists(self.path):
                        self.fs.rm(self.path, recursive=True)

                    fs.mv(self.temp_path, self.path, recursive=True)

                    # Reopen the dataset - will use the appropriate global or local cache
                    return xr.open_dataset(self.path, engine='zarr',
                                           chunks={}, decode_timedelta=True)
                else:
                    # Requested chunks already match rechunk.
                    return xr.open_dataset(self.path, engine='zarr',
                                           chunks={}, decode_timedelta=True)
            else:
                return xr.open_dataset(self.path, engine='zarr', chunks={}, decode_timedelta=True)
        else:
            raise NotImplementedError(f"Zarr backend does not support reading zarrs to {engine} engine")


    def chunk_to_zarr(self, ds, path):
        """Write a dataset to a zarr cache map and check the chunking."""
        ds = drop_encoded_chunks(ds)

        if isinstance(self.chunking, dict):
            # No need to prune if chunking is None or 'auto'
            chunking = prune_chunking_dimensions(ds, self.chunking)

        ds = ds.chunk(chunks=chunking)

        try:
            chunk_size, chunk_with_labels = get_chunk_size(ds)

            if chunk_size > CHUNK_SIZE_UPPER_LIMIT_MB or chunk_size < CHUNK_SIZE_LOWER_LIMIT_MB:
                print(f"WARNING: Chunk size is {chunk_size}MB. Target approx 100MB.")
                print(chunk_with_labels)
        except ValueError:
            print("Failed to get chunks size! Continuing with unknown chunking...")

        if fs.exists(path):
            fs.rm(path, recursive=True)

        ds.to_zarr(store=path, mode='w')


