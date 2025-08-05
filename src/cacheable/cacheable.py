"""Automated dataframe caching utilities."""
import os
import inspect
from functools import wraps
from inspect import signature, Parameter
import logging
import hashlib


logger = logging.getLogger(__name__)

CHUNK_SIZE_UPPER_LIMIT_MB = 300
CHUNK_SIZE_LOWER_LIMIT_MB = 30

# Global variables for caching configuration
global_recompute = None
global_force_overwrite = None
global_retry_null_cache = None

# Remote dir for all caches, except for postgres and terracotta
CACHE_ROOT_DIR = "gs://sheerwater-datalake/caches/"
CACHE_STORAGE_OPTIONS = {
    'token': 'google_default',
    'cache_timeout': 0
}
POSTGRES_IP = "postgres.sheerwater.rhizaresearch.org"
# Local dir for all caches, except for postgres and terracotta
LOCAL_CACHE_ROOT_DIR = os.path.expanduser("~/.cache/sheerwater/caches/")
LOCAL_CACHE_STORAGE_OPTIONS = {}

# Check if these are defined as environment variables
if 'SHEERWATER_CACHE_ROOT_DIR' in os.environ:
    CACHE_ROOT_DIR = os.environ['SHEERWATER_CACHE_ROOT_DIR']
if 'SHEERWATER_LOCAL_CACHE_ROOT_DIR' in os.environ:
    LOCAL_CACHE_ROOT_DIR = os.environ['SHEERWATER_LOCAL_CACHE_ROOT_DIR']


def set_global_cache_variables(recompute=None, force_overwrite=None,
                               retry_null_cache=None):
    """Reset all global variables to defaults and set the new values."""
    global global_recompute, global_force_overwrite, \
        global_validate_cache_timeseries, global_retry_null_cache

    # Simple logic for global variables
    global_force_overwrite = force_overwrite
    global_retry_null_cache = retry_null_cache

    # More complex logic for recompute
    if recompute == True:  # noqa: E712
        global_recompute = False
    elif isinstance(recompute, str) and recompute != '_all':
        # if a single function's name is passed, convert to a list
        global_recompute = [recompute]
    else:
        global_recompute = recompute  # if recompute is false, '_all' or a list


def check_if_nested_fn():
    """Check if the current scope downstream from another cached function."""
    # Get the current frame
    stack = inspect.stack()
    # skip the first two frames (this function and the current cacheable function)
    for frame_info in stack[2:]:
        frame = frame_info.frame
        func_name = frame.f_code.co_name
        if func_name == "cacheable_wrapper":
            # There is a cachable function upstream of this one
            return True
    # No cachable function upstream of this one
    return False


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


def get_local_cache(cache_path):
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
    return os.path.join(LOCAL_CACHE_ROOT_DIR, cache_key)


def sync_local_remote(backend, cache_fs, local_fs,
                      cache_path=None, local_cache_path=None,
                      verify_path=None, null_path=None):
    """Sync a local cache mirror to a remote cache.

    Args:
        backend (str): The backend to use for the cache
        cache_fs (fsspec.core.url_to_fs): The filesystem of the cache
        local_fs (fsspec.core.url_to_fs): The filesystem of the local mirror
        cache_path (str): The path to the cache
        local_cache_path (str): The path to the local cache
        verify_path (str): The path to the verify file
        null_path (str): The path to the null file
    """
    if local_cache_path == cache_path:
        return  # don't sync if the read and the remote filesystems are the same

    # Syncing is only possible if the remote cache exists and is valid
    assert backend in SUPPORTS_LOCAL_CACHING
    assert cache_exists(backend, cache_path, verify_path)

    local_verify_path = get_local_cache(verify_path)
    local_null_path = get_local_cache(null_path)

    # Remove the local null cache if it doesn't exist on remote
    if not cache_fs.exists(null_path) and local_fs.exists(local_null_path):
        local_fs.rm(local_null_path, recursive=True)

    # If the cache exists locally and is valid, we're synced - return!
    if cache_exists(backend, local_cache_path, local_verify_path):
        # If we have a local cache, check and make sure the verify timestamp is the same
        local_verify_ts = local_fs.open(local_verify_path, 'r').read()
        remote_verify_ts = cache_fs.open(verify_path, 'r').read()
        if local_verify_ts == remote_verify_ts:
            return

    # If the verify timestamp is different, or it doesn't exist, delete the local cache
    # and sync the remote cache to local
    if cache_path and cache_fs.exists(cache_path):
        if local_fs.exists(local_cache_path):
            local_fs.rm(local_cache_path, recursive=True)
        cache_fs.get(cache_path, local_cache_path, recursive=True)
    if verify_path and cache_fs.exists(verify_path):
        if local_fs.exists(local_verify_path):
            local_fs.rm(local_verify_path)
        cache_fs.get(verify_path, local_verify_path)
    if null_path and cache_fs.exists(null_path):
        if local_fs.exists(local_null_path):
            local_fs.rm(local_null_path)
        cache_fs.get(null_path, local_null_path)


def get_cache_args(kwargs, cache_kwargs):
    """Extract the cache arguments from the kwargs and return them."""
    cache_args = []
    for k in cache_kwargs:
        if k in kwargs:
            cache_args.append(kwargs[k])
            del kwargs[k]
        else:
            cache_args.append(cache_kwargs[k])
    return cache_args



def check_cache_disable_if(cache_disable_if, cache_arg_values):
    """Check if the cache should be disabled for the given kwargs.

    Cache disable if is a dict or list of dicts. Each dict specifies a set of
    arguments that should disable the cache if they are present.
    """
    if isinstance(cache_disable_if, dict):
        cache_disable_if = [cache_disable_if]
    elif isinstance(cache_disable_if, list):
        pass
    else
        raise ValueError("cache_disable_if only accepts a dict or list of dicts.")

    for d in cache_disable_if:
        if not isinstance(d, dict):
            raise ValueError("cache_disable_if only accepts a dict or list of dicts.")

        # Get the common keys
        common_keys = set(cache_arg_values).intersection(d)

        # Remove any args not passed
        comp_arg_values = {key: cache_arg_values[key] for key in common_keys}
        d = {key: d[key] for key in common_keys}

        # Iterate through each key and check if the values match, with support for lists
        key_match = [
            (not isinstance(d[k], list) and comp_arg_values[k] == d[k]) or
            (isinstance(d[k], list) and comp_arg_values[k] in d[k])
            for k in common_keys
        ]
        # Within a cache disable if dict, if all keys match, disable the cache
        if all(key_match):
            print(f"Caching disabled for arg values {d}")
            return False
    # Keep the cache enabled - we didn't find a match
    return True


def extract_cache_arg_values(cache_args, params, kwargs):
    # Handle keying based on cache arguments
    cache_arg_values = {}

    for a in cache_args:
        # If it's in kwargs, great
        if a in kwargs:
            cache_arg_values[a] = kwargs[a]
            continue

        # If it's not in kwargs it must either be (1) in args or (2) passed as default
        found = False
        for i, p in enumerate(params):
            if (a == p and len(args) > i and
                    (params[p].kind == Parameter.VAR_POSITIONAL or
                     params[p].kind == Parameter.POSITIONAL_OR_KEYWORD)):
                cache_arg_values[a] = args[i]
                found = True
                break
            elif a == p and params[p].default != Parameter.empty:
                cache_arg_values[a] = params[p].default
                found = True
                break

        if not found:
            raise RuntimeError(f"Specified cacheable argument {a} "
                               "not discovered as passed argument or default argument.")

    return cache_arg_values

def get_cache_key(func, cache_arg_values):
    imkeys = list(cache_arg_values.keys())
    imkeys.sort()
    sorted_values = [cache_arg_values[i] for i in imkeys]
    flat_values = []
    for val in sorted_values:
        if isinstance(val, list):
            sub_vals = [str(v) for v in val]
            sub_vals.sort()
            flat_values += sub_vals
        elif isinstance(val, dict):
            sub_vals = [f"{k}-{v}" for k, v in val.items()]
            sub_vals.sort()
            flat_values += sub_vals
        else:
            flat_values.append(str(val))

    return func.__name__ + '/' + '_'.join(flat_values)


def get_backend_types(metadata_backend, backend, storage_backend):
    prior_backend_type = None
    if metadata_backend.cache_exists():
        prior_backend_type = metadata_backend.get_backend()

    read_backend_type = backend
    write_backend_type = storage_backend

    if write_backend_type is None and read_backend_type:
        write_backend_type = read_backend_type
    elif write_backend_type is None and prior_backend_type:
        write_backend_type = prior_backend_type

    if read_backend_type is None and prior_backend_type:
        read_backend_type = prior_backend_type

    return read_backend_type, write_backend_type


def cacheable(cache=True,
              cache_args,
              cache_disable_if=None,
              backend=None,
              storage_backend=None,
              cache_local=False,
              engine=None,
              primary_keys=None):
    """Decorator for caching function results.

    Args:
        cache_args(list): The arguments to use as the cache key.
        backend_kwargs(dict): A dictionary of backend-specific arguments that will be passed to
            and used back the backend for writign and reading
                cache(bool): Whether to cache the result.
        force_overwrite(bool): Whether to overwrite the cache if it
            already exists (if False, will prompt the user before overwriting).
        retry_null_cache(bool): If True, ignore and delete the null caches and attempts to recompute
            result for null values. If False (default), will return None for null caches.
        cache_disable_if(dict, list): If the cache arguments match the dict or list of dicts
            then the cache will be disabled. This is useful for disabling caching based on
            certain arguments. Defaults to None.
        backend(str): The name of the backend to use for cache recall/storage. None for
            default, zarr, delta, postgres, terracotta.
        storage_backend(str): The name of the backend to use for cache storage only. None
            to match backend. Useful for pulling from one backend and writing to another.
        cache_local (bool): If True, will mirror the result locally, at the location
            specified by the LOCAL_CACHE_ROOT_DIR variable. Default is False.
        primary_keys (list(str)): Column names of the primary keys to user for upsert.
    """
    # Valid configuration kwargs for the cacheable decorator
    cache_kwargs = {
        "filepath_only": False,
        "recompute": False,
        "cache": None,
        "force_overwrite": None,
        "retry_null_cache": False,
        "backend": None,
        "storage_backend": None,
        "cache_local": False,
        "upsert": False,
        "fail_if_no_cache": False,
    }

    nonlocals = locals()

    def create_cacheable(func):

        @wraps(func)
        def cacheable_wrapper(*args, **kwargs):
            # Proper variable scope for the decorator args
            data_type = nonlocals['data_type']
            cache_args = nonlocals['cache_args']
            cache = nonlocals['cache']
            cache_disable_if = nonlocals['cache_disable_if']
            backend = nonlocals['backend']
            storage_backend = nonlocals['storage_backend']
            cache_local = nonlocals['cache_local']
            primary_keys = nonlocals['primary_keys']

            # Calculate the appropriate cache key
            filepath_only, recompute, passed_cache, \
                force_overwrite, retry_null_cache, passed_backend, \
                storage_backend, passed_cache_local, \
                upsert, \
                fail_if_no_cache = get_cache_args(kwargs, cache_kwargs)

            if passed_cache is not None:
                cache = passed_cache
            if passed_backend is not None:
                backend = passed_backend
            if passed_cache_local is not None:
                cache_local = passed_cache_local

            # Check if this is a nested cacheable function
            if not check_if_nested_fn():
                # This is a top level cacheable function, reset global cache variables
                set_global_cache_variables(recompute=recompute,
                                           force_overwrite=force_overwrite,
                                           retry_null_cache=retry_null_cache)
                if isinstance(recompute, list) or isinstance(recompute, str) or recompute == '_all':
                    recompute = True
            else:
                # Inherit global cache variables
                global global_recompute, global_force_overwrite, global_retry_null_cache

                # Set all global variables
                if global_force_overwrite is not None:
                    force_overwrite = global_force_overwrite
                if global_retry_null_cache is not None:
                    retry_null_cache = global_retry_null_cache
                if global_recompute:
                    if func.__name__ in global_recompute or global_recompute == '_all':
                        recompute = True

            # The the function parameters and their values
            params = signature(func).parameters
            cache_arg_values = extract_cache_arg_values(cache_args, params, kwargs)

            # Disable the cache if it's enabled and the function params/values match the disable statement
            if cache:
                cache = check_cache_disable_if(cache_disable_if, cache_arg_values)

            # Calculate our unique cache key from the params and values
            cache_key = get_cache_key(func, cache_arg_values)

            ds = None
            compute_result = True

            # Instantiate a metadata backend - metadata is just a backend like all others but with some extra methods
            metadata_backend = MetadataBackend(cache_config, cache_key, cache_arg_values, backend_kwargs)

            # Check to see if there is already a cached null that is valid
            if metadata_backend.cache_exists():
                if metadata_backend.null():
                    if retry_null_cache:
                        # Remove the null and move on because we don't know the backend and need to recompute
                        metadata_backend.delete_null())
                    elif recompute:
                        # Just move on, let the user decide whether to oeverwrite the null cache later
                        pass
                    elif cache:
                        # We want the null cache, return none
                        print(f"Found null cache for {null_path}. Skipping computation.")
                        return None
                    else:
                        # This implies cache is false, so we won't return the cache value
                        # we will recompute and not cache the values

            # Set up the backends if we have the information
            read_backend_type, write_backend_type = get_backend_types(metadata_backend, backend, storage_backend)

            # Make core versions of the backends if we can
            if read_backend_type:
                read_backend_class = backend_class_map[read_backend_type]
                read_backend = backend_class(cache_key, backend_fs, cache_arg_values, backend_kwargs)

                if local_mirror_fs:
                    local_read_backend = backend_class(cache_key, local_mirror_fs, cache_arg_values, backend_kwargs)

            if write_backend_type:
                write_backend_class = backend_class_map[write_backend_type]
                write_backend = backend_class(cache_key, backend_fs, cache_arg_values, backend_kwargs)

                if local_mirror_fs:
                    local_write_backend = backend_class(cache_key, local_mirror_fs, cache_arg_values, backend_kwargs)


            # Read the cache from the appropriate location if it exists
            if not recompute and not upsert and cache and read_backend:
                if read_backend.cache_exists()
                    # Sync the cache from the remote to the local if necessary
                    sync_local_remote(read_backend, local_read_backend)

                    if local_mirror_backend:
                        print(f"Found cache for {backend.get_cache_path()}")
                        if filepath_only:
                            return local_read_backend.get_cache_path()
                        else:
                            print(f"Opening cache {read_backend.get_cache_path()}")
                            ds = local_read_backend.read()
                            compute_result = False
                    else:
                        print(f"Found cache for {backend.get_cache_path()}")
                        if filepath_only:
                            return read_backend.get_cache_path()
                        else:
                            print(f"Opening cache {read_backend.get_cache_path()}")
                            ds = read_backend.read()
                            compute_result = False

                    # Retry to preform the sync because for some backends data is transformed on read(!)
                    sync_local_remote(read_backend, local_read_backend)


            # If the cache doesn't exist or we are recomputing, compute the result
            if compute_result:
                if recompute:
                    print(f"Recompute for {cache_key} requested. Not checking for cached result.")
                elif upsert:
                    print(f"Computing {cache_key} to enable data upsert.")
                elif not cache:
                    # The function isn't cacheable, recomputing
                    pass
                else:
                    if fail_if_no_cache:
                        raise RuntimeError(f"""Computation has been disabled by
                                            `fail_if_no_cache` and cache doesn't exist for {cache_key}.""")

                    print(f"Cache doesn't exist for {cache_key}. Running function")

                ##### IF NOT EXISTS ######
                ds = func(*args, **kwargs)
                ##########################

            # Store the result
            if cache and (compute_result or read_backend_type != write_backend_type):
                if ds is None:
                    if not upsert:
                        metadata_backend.write_null()
                        write_backend.write_null()
                        sync_local_remote(write_backend, local_write_backend)
                    else:
                        print(f"Null result not cached for {null_path} in upsert mode.")

                    return None

                write = False  # boolean to determine if we should write to the cache
                if (write_backend.cache_exists() and force_overwrite is None and not upsert):
                    inp = input(f'A cache already exists at {
                                write_backend.get_cache_path()}. Are you sure you want to overwrite it? (y/n)')
                    if inp == 'y' or inp == 'Y':
                        write = True
                elif force_overwrite is False:
                    pass
                else: write = True

                if write:
                    print(f"Caching result for {write_backend.get_cache_path()} as zarr.")
                    write_backend.write(ds, upsert=upsert, primary_keys=primary_keys)
                    sync_local_remote(write_backend, local_write_backend)

                    ds = write_backend.read(engine)

            if filepath_only:
                return write_backend.get_cache_path()
            else:
                return ds

        return cacheable_wrapper
    return create_cacheable
