from sheerwater_benchmarking.utils.backends import CacheableBackend
CHUNK_SIZE_UPPER_LIMIT_MB = 300
CHUNK_SIZE_LOWER_LIMIT_MB = 30

def get_temp_cache(cache_path):
    """Get the local cache path based on the file system.

    Args:
        cache_path (str): Path to cache file

    Returns:
        str: Local cache path
    """
    if cache_path is None:
        return None
    if not cache_path.startswith(CACHE_ROOT_DIR):
        raise ValueError("Cache path must start with CACHE_ROOT_DIR")

    cache_key = cache_path.split(CACHE_ROOT_DIR)[1]
    return os.path.join(CACHE_ROOT_DIR, 'temp', cache_key)





def write_to_zarr(ds, cache_path, verify_path):
    """Write to zarr with a temp write and move to make it more atomic.

    # If you were to make this atomic this is what it would look like:
    lock = verify_path + ".lock"

    storage_client = storage.Client()
    blob = Blob.from_string(lock, client=storage_client)

    try:
        blob.upload_from_string("lock", if_generation_match=0)
    except google.api_core.exceptions.PreconditionFailed:
        raise RuntimeError(f"Concurrent zarr write detected. If this is a mistake delete the lock file: {lock}")

    fs.rm(lock)
    """
    fs = fsspec.core.url_to_fs(cache_path, **CACHE_STORAGE_OPTIONS)[0]
    if fs.exists(verify_path):
        fs.rm(verify_path, recursive=True)

    if fs.exists(cache_path):
        fs.rm(cache_path, recursive=True)

    cache_map = fs.get_mapper(cache_path)
    ds.to_zarr(store=cache_map, mode='w')

    # Add a lock file to the cache to verify cache integrity, with the current timestamp
    fs.open(verify_path, 'w').write(datetime.datetime.now(datetime.timezone.utc).isoformat())


def chunk_to_zarr(ds, cache_path, verify_path, chunking):
    """Write a dataset to a zarr cache map and check the chunking."""
    ds = drop_encoded_chunks(ds)

    if isinstance(chunking, dict):
        # No need to prune if chunking is None or 'auto'
        chunking = prune_chunking_dimensions(ds, chunking)
    ds = ds.chunk(chunks=chunking)
    try:
        chunk_size, chunk_with_labels = get_chunk_size(ds)

        if chunk_size > CHUNK_SIZE_UPPER_LIMIT_MB or chunk_size < CHUNK_SIZE_LOWER_LIMIT_MB:
            print(f"WARNING: Chunk size is {chunk_size}MB. Target approx 100MB.")
            print(chunk_with_labels)
    except ValueError:
        print("Failed to get chunks size! Continuing with unknown chunking...")
    write_to_zarr(ds, cache_path, verify_path)



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



class ZarrBackend(CacheableBackend):
    """
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


            # Update chunking based on chunk_by_arg
            chunking = merge_chunk_by_arg(chunking, chunk_by_arg, cache_arg_values)

    def write(data):

        if isinstance(ds, xr.Dataset):
                                chunk_config = chunking if chunking else 'auto'
                                chunk_to_zarr(ds, cache_path, verify_path, chunk_config)
                                # Reopen the dataset to truncate the computational path
                                ds = xr.open_dataset(read_cache_map, engine='zarr',
                                                     chunks={}, decode_timedelta=True)

        pass

    def read(engine):

                        # We must auto open chunks. This tries to use the underlying zarr chunking if possible.
                        # Setting chunks=True triggers what I think is an xarray/zarr engine bug where
                        # every chunk is only 4B!
        if auto_rechunk:
                            # If rechunk is passed then check to see if the rechunk array
                            # matches chunking. If not then rechunk
                            ds_remote = xr.open_dataset(cache_map, engine='zarr', chunks={}, decode_timedelta=True)
                            if not isinstance(chunking, dict):
                                raise ValueError(
                                    "If auto_rechunk is True, a chunking dict must be supplied.")

                            # Compare the dict to the rechunk dict
                            if not chunking_compare(ds_remote, chunking):
                                print(
                                    "Rechunk was passed and cached chunks do not match rechunk request. "
                                    "Performing rechunking.")

                                # write to a temp cache map
                                # writing to temp cache is necessary because if you overwrite
                                # the original cache map it will write it before reading the
                                # data leading to corruption.
                                temp_cache_path = CACHE_ROOT_DIR + 'temp/' + cache_key + '.temp'
                                temp_verify_path = CACHE_ROOT_DIR + 'temp/' + cache_key + '.verify'
                                chunk_to_zarr(ds_remote, temp_cache_path, temp_verify_path, chunking)

                                # Remove the old cache and verify files
                                if fs.exists(verify_path):
                                    fs.rm(verify_path, recursive=True)
                                if fs.exists(cache_path):
                                    fs.rm(cache_path, recursive=True)

                                fs.mv(temp_cache_path, cache_path, recursive=True)
                                fs.mv(temp_verify_path, verify_path, recursive=True)

                                # Sync the cache from the remote to the local
                                sync_local_remote(backend, fs, read_fs, cache_path,
                                                  read_cache_path, verify_path, null_path)

                                # Reopen the dataset - will use the appropriate global or local cache
                                ds = xr.open_dataset(read_cache_map, engine='zarr',
                                                     chunks={}, decode_timedelta=True)
                            else:
                                # Requested chunks already match rechunk.
                                ds = xr.open_dataset(read_cache_map, engine='zarr',
                                                     chunks={}, decode_timedelta=True)
                        else:
                            ds = xr.open_dataset(read_cache_map, engine='zarr', chunks={}, decode_timedelta=True)


        pass

    def exists(engine):
        pass


