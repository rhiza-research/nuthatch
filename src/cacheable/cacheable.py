"""Automated dataframe caching utilities."""
import os
import inspect
from functools import wraps
from inspect import signature, Parameter
from backends import get_backend
from metadata import CacheMetadata
from config import get_config
import logging
import hashlib


logger = logging.getLogger(__name__)

# Global variables for caching configuration
global_recompute = None
global_force_overwrite = None
global_retry_null_cache = None


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


def sync_local_remote(backend, local_backend)
    """Sync a local cache mirror to a remote cache.

    Args:
        backend (CacheableBackend): The backend to use for the cache
        local_backend (CacheableBackend): The backend to use for the local cache
    """

    # If there is a local
    if not local_backend:
        return

    assert backend.exists()
    backend.sync(local_backend)


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



def get_backend_types(metadata, backend, backend_kwargs, storage_backend, storage_backend_kwargs):
    prior_backend_type = None
    if metadata.exists():
        prior_backend_type = metadata.get_backend()

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
              engine=None,
              backend=None,
              backend_kwargs=None,
              storage_backend=None,
              storage_backend_kwargs=None,
              cache_local=False,
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
        "cache": None,
        "backend": None,
        "backend_kwargs": None,
        "cache_local": False,
        "storage_backend": None,
        "storage_backend_kwargs": None,
        "filepath_only": False,
        "recompute": False,
        "force_overwrite": None,
        "retry_null_cache": False,
        "upsert": False,
        "fail_if_no_cache": False,
    }

    nonlocals = locals()

    def create_cacheable(func):

        @wraps(func)
        def cacheable_wrapper(*args, **kwargs):
            # Proper variable scope for the decorator args
            cache_args = nonlocals['cache_args']
            cache = nonlocals['cache']
            cache_disable_if = nonlocals['cache_disable_if']
            backend = nonlocals['backend']
            backend_kwargs = nonlocals['backend_kwargs']
            storage_backend = nonlocals['storage_backend']
            storage_backend_kwargs = nonlocals['storage_backend_kwargs']
            cache_local = nonlocals['cache_local']
            primary_keys = nonlocals['primary_keys']

            # Calculate the appropriate cache key
            passed_cache, passed_backend, passed_backend_kwargs, \
            passed_cache_local, storage_backend, storage_backend_kwargs, \
            filepath_only, recompute, \
            force_overwrite, retry_null_cache, upsert, \
            fail_if_no_cache = get_cache_args(kwargs, cache_kwargs)

            if passed_cache is not None:
                cache = passed_cache
            if passed_backend is not None:
                backend = passed_backend
            if passed_backend_kwargs is not None:
                if backend_kwargs is None:
                    backend_kwargs = passed_backend_kwargs
                else:
                    backend_kwargs = backend_kwargs.update(passed_backend_kwargs)
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
            metadata = CacheMetadata(get_config(location='root', backend_class=CacheMetadata), cache_key)

            # Check to see if there is already a cached null that is valid
            if metadata.exists():
                if metadata_backend.is_null():
                    if retry_null_cache:
                        # Remove the null and move on because we don't know the backend and need to recompute
                        metadata.delete_null())
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
            read_backend_type, read_backend_kwargs, write_backend_type, \
                write_backend_kwargs = get_backend_types(metadata, backend, backend_kwargs, storage_backend, storage_backend_kwargs)

            # Make core versions of the backends if we can
            if read_backend_type:
                read_backend_class = get_backend(read_backend_type)
                read_backend = backend_class(get_config(location='root', backend_class=read_backend_class),
                                             cache_key, cache_arg_values, read_backend_kwargs)

                local_config = get_config(location='local', backend_class=read_backend_class)
                if local:
                    if local_config
                        local_read_backend = backend_class(local_config, cache_key, cache_arg_values, read_backend_kwargs)
                    else:
                        raise RuntimeError("Local backend not configured. Configure local backend for mirrong.")

            if write_backend_type:
                write_backend_class = get_backend(write_backend_type)
                write_backend = write_backend_class(get_config(location='root', backend_class=write_backend_class),
                                              cache_key, cache_arg_values, write_backend_kwargs)

                local_write_config = get_config(location='local', backend_class=write_backend_class)
                if local_write_config and local:
                    local_write_backend = backend_class(local_write_config, cache_key, cache_arg_values, write_backend_kwargs)


            # Read the cache from the appropriate location if it exists
            if not recompute and not upsert and cache and read_backend:
                if read_backend.cache_exists()
                    # Sync the cache from the remote to the local if necessary
                    sync_local_remote(read_backend, local_read_backend)

                    if local_read_backend:
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
                        metadata.write_null()
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
