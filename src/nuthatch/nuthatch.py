"""The main module for the nuthatch package.

This module contains the main decorator for caching functions and the global
variables for caching configuration.
"""
from functools import wraps, partial
import inspect
from inspect import signature, Parameter
import logging
import sys

from nuthatch.cache import Cache
from nuthatch.backend import get_default_backend
from nuthatch.config import NuthatchConfig
from nuthatch.config import set_global_skipped_filesystem
from nuthatch.memoizer import save_to_memory, recall_from_memory

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(module)s %(levelname)s: %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)

# Global variables for caching configuration
global_recompute = None
global_memoize = None
global_cache_mode = None
global_retry_null_cache = None
global_fs_warning = []


def set_global_cache_variables(recompute=None, memoize=None, cache_mode=None, retry_null_cache=None):
    """Reset all global variables to defaults and set the new values."""
    global global_recompute, global_memoize, global_cache_mode, \
        global_retry_null_cache

    # Simple logic for global variables
    global_retry_null_cache = retry_null_cache
    global_cache_mode = cache_mode

    # More complex logic for recompute
    if recompute == True:  # noqa: E712, must check the boolean
        global_recompute = False
    elif isinstance(recompute, str) and recompute != '_all':
        # if a single function's name is passed, convert to a list
        global_recompute = [recompute]
    else:
        global_recompute = recompute  # if recompute is false, '_all' or a list

    # More complex logic for memoize
    if memoize == True:  # noqa: E712
        global_memoize = False
    elif isinstance(memoize, str) and memoize != '_all':
        # if a single function's name is passed, convert to a list
        global_memoize = [memoize]
    else:
        global_memoize = memoize  # if memoize is false, '_all' or a list


def check_if_nested_fn():
    """Check if the current scope is downstream from another cached function."""
    # Get the current frame
    stack = inspect.stack()

    # skip the first three frames (this function, check args, and the current cacheable function)
    for frame_info in stack[3:]:
        frame = frame_info.frame
        func_name = frame.f_code.co_name
        if func_name == "nuthatch_cacheable_wrapper":
            # There is a cachable function upstream of this one
            return True
    # No cachable function upstream of this one
    return False


def get_cache_args(passed_kwargs, default_cache_kwargs, decorator_args, func_name):
    """Extract the cache arguments from the kwargs and return them."""
    cache_args = {}
    for k in default_cache_kwargs:
        if k in passed_kwargs:
            cache_args[k] = passed_kwargs[k]
            del passed_kwargs[k]
        else:
            cache_args[k] = default_cache_kwargs[k]

    if cache_args['namespace'] is None:
        cache_args['namespace'] = decorator_args['namespace']
    if cache_args['engine'] is None:
        cache_args['engine'] = decorator_args['engine']
    if cache_args['memoize'] is None:
        cache_args['memoize'] = decorator_args['memoize']
    if cache_args['backend'] is None:
        cache_args['backend'] = decorator_args['backend']
    # Cache can only be passed into the cacheable decorator, not at runtime
    cache_args['cache'] = decorator_args['cache']

    if 'backend_kwargs' in cache_args and isinstance(cache_args['backend_kwargs'], dict):
        cache_args['backend_kwargs'] = cache_args['backend_kwargs'].update(decorator_args['backend_kwargs'])
    elif 'backend_kwargs' in cache_args and cache_args['backend_kwargs'] is None:
        cache_args['backend_kwargs'] = decorator_args['backend_kwargs']

    # Check if this is a nested cacheable function
    if not check_if_nested_fn():
        # This is a top level cacheable function, reset global cache variables
        set_global_cache_variables(recompute=cache_args['recompute'], memoize=cache_args['memoize'],
                                   cache_mode=cache_args['cache_mode'], retry_null_cache=cache_args['retry_null_cache'])
        if isinstance(cache_args['recompute'], list) or isinstance(cache_args['recompute'], str) or cache_args['recompute'] == '_all':
            cache_args['recompute'] = True
        if isinstance(cache_args['memoize'], list) or isinstance(cache_args['memoize'], str) or cache_args['memoize'] == '_all':
            cache_args['memoize'] = True
    else:
        # Inherit global cache variables
        global global_recompute, global_memoize, global_cache_mode, global_retry_null_cache

        # Set all global variables
        if global_retry_null_cache is not None:
            cache_args['retry_null_cache'] = global_retry_null_cache
        if global_cache_mode is not None:
            cache_args['cache_mode'] = global_cache_mode
        if global_recompute:
            if func_name in global_recompute or global_recompute == '_all':
                cache_args['recompute'] = True
        if global_memoize:
            if func_name in global_memoize or global_memoize == '_all':
                cache_args['memoize'] = True

    return cache_args


def check_cache_disable_if(cache_disable_if, cache_arg_values):
    """Check if the cache should be disabled for the given kwargs.

    Cache disable if is a dict or list of dicts. Each dict specifies a set of
    arguments that should disable the cache if they are present.
    """
    if not cache_disable_if:
        return True

    if isinstance(cache_disable_if, dict):
        cache_disable_if = [cache_disable_if]
    elif isinstance(cache_disable_if, list):
        pass
    else:
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
            logger.info(f"Caching disabled for arg values {d}")
            return False

    # Keep the cache enabled - we didn't find a match
    return True


def extract_cache_arg_values(cache_args, args, params, kwargs):
    """Extract the cache arguments from the kwargs and return them."""
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


def extract_all_arg_values(args, params, kwargs):
    """Extract the cache arguments from the kwargs and return them."""
    # Handle keying based on cache arguments
    all_arg_values = {}

    def add_arg(a):
        return (isinstance(a, int) or isinstance(a, float) or
                isinstance(a, str) or isinstance(a, bool) or
                len(str(a)) < 30)

    for a in kwargs:
        if add_arg(kwargs[a]):
            all_arg_values[a] = kwargs[a]

    # If it's not in kwargs it must either be (1) in args or (2) passed as default
    for i, p in enumerate(params):
        if p in all_arg_values:
            continue

        if (len(args) > i and
           (params[p].kind == Parameter.VAR_POSITIONAL or
           params[p].kind == Parameter.POSITIONAL_OR_KEYWORD)):
            if add_arg(args[i]):
                all_arg_values[p] = args[i]
        elif params[p].default != Parameter.empty:
            if add_arg(params[p].default):
                all_arg_values[p] = params[p].default

    return all_arg_values


def get_cache_key(func, cache_arg_values):
    """Calculate the cache key from the function and the cache arguments."""
    imkeys = list(cache_arg_values.keys())
    imkeys.sort()
    sorted_values = [cache_arg_values[i] for i in imkeys]
    flat_values = []
    imvalues = []

    for val in sorted_values:
        if isinstance(val, list):
            sub_vals = [str(v) for v in val]
            sub_vals.sort()
            flat_values += sub_vals
            imvalues.append(sub_vals)
        elif isinstance(val, dict):
            sub_vals = [f"{k}-{v}" for k, v in val.items()]
            sub_vals.sort()
            flat_values += sub_vals
            imvalues.append(sub_vals)
        else:
            flat_values.append(str(val))
            imvalues.append(str(val))

    cache_key = func.__name__ + '/' + '_'.join(flat_values)
    cache_print = func.__name__ + '/' + '/'.join([f'{x[0]}_{x[1]}' for x in zip(imkeys, imvalues)])
    return cache_key, cache_print


def instantiate_read_caches(config, cache_key, namespace, version, cache_arg_values, requested_backend, backend_kwargs):
    """Returns a priority ordered list of caches to read from.

    Args:
        cache_key (str): The cache key.
        namespace (str): The namespace.
        version (str): The cache version.
        cache_arg_values (dict): The cache arguments.
        requested_backend (str): The requested backend.
        backend_kwargs (dict): The backend kwargs.

    Returns:
        caches (dict): A dictionary of caches to read from.
        mirror_exception (str): An exception message if we have a mirror exception.
    """
    # The general order of priority to check validity is:
    # (1) local if local is requested and local config is provided
    # (2) the root cache
    # (3) any mirror caches

    # Start by trying to instantiate the metadata stores then do our best to instantiate the backends themselves
    caches = {}
    found_cache = False

    mirror_exception = None
    global global_fs_warning
    for location, location_values in config.items():
        # If the cache is local, we don't need to instantiate it
        if location == 'local':
            continue

        cache = None
        if location.startswith('mirror'):
            # First, try to set up a mirror (read-only) cache
            if 'filesystem' in location_values:
                # Check if this filesystem is listed to be skipped in the cache configuration
                if 'skipped_filesystems' in config['root'] and location_values['filesystem'] in config['root']['skipped_filesystems']:
                    if location_values['filesystem'] not in global_fs_warning:
                        logger.warning(
                            f"Skipping filesystem {location_values['filesystem']} because it has been added to the excluded filesystems list in ~/.nuthatch.toml. Please remove it if you now have access.")
                        global_fs_warning.append(location_values['filesystem'])
                    continue

                try:
                    # If this is not a skipped filesystem, try to instantiate the cache
                    cache = Cache(location_values, cache_key, namespace, version,
                                  cache_arg_values, requested_backend, backend_kwargs)
                    caches[f"{location}"] = cache
                    found_cache = True
                except Exception as e:  # noqa
                    logger.warning(
                        f"Failed to access the cache at {location_values['filesystem']}. Adding it to the excluded filesystems list at ~/.nuthatch.toml so future runs are faster. Please remove it if you gain access in the future.")
                    set_global_skipped_filesystem(
                        location_values['filesystem'])
                    global_fs_warning.append(location_values['filesystem'])
                    mirror_exception = f'Failed to access configured nuthatch mirror "{location}" with error "{type(e).__name__}: {e}". If you couldn`t access the expected data, this could be the reason.'
        else:
            if 'filesystem' in location_values:
                try:
                    # Try to instantiate the read-write root cache
                    cache = Cache(location_values, cache_key, namespace, version,
                                  cache_arg_values, requested_backend, backend_kwargs)
                    caches[location] = cache
                    found_cache = True
                except Exception as e:
                    # If we can't instantiate the root cache, raise an error
                    raise RuntimeError(
                        f'Nuthatch is unable to access the configured root cache at {location_values["filesystem"]} with error "{type(e).__name__}: {e}." If you configure a root cache it must be accessible. Please ensure that you have correct access credentials for the configured root cache.')

    if not found_cache:
        raise RuntimeError("No Nuthatch configuration has been found.\n"
                           "-> If you are developing a Nuthatch project, please configure nuthatch in your pyproject.toml\n"
                           "-> If you are calling a project that uses nuthatch, it should just work! Please contact the project's maintainer.")

    # Move root to beginning of the ordered dictionary of caches to check
    if 'root' in caches:
        root = caches.pop('root')
        caches = {'root': root, **caches}

    return (caches, mirror_exception)


def instantiate_local_read_cache(config, cache_key, namespace, version, cache_arg_values, requested_backend, backend_kwargs):
    """Instantiate the local read/write cache."""
    try:
        return Cache(config['local'], cache_key, namespace, version, cache_arg_values, requested_backend, backend_kwargs)
    except Exception as e:
        raise RuntimeError(
            f'Nuthatch is unable to access the configured local cache at {config["local"]["filesystem"]} with error "{type(e).__name__}": {e}.')


def get_storage_backend(ds, backend, backend_kwargs, storage_backend, storage_backend_kwargs):
    """Get the appropriate storage backend a specific dataset, based on the default backend for the dataset type."""
    # get the appropriate storage backend
    if not storage_backend and not backend:
        storage_backend = get_default_backend(type(ds))
        if not storage_backend_kwargs:
            storage_backend_kwargs = backend_kwargs
    elif backend:
        storage_backend = backend
        storage_backend_kwargs = backend_kwargs

    return storage_backend, storage_backend_kwargs


def instantiate_write_caches(config, cache_key, memoizer_cache_key, namespace, version, cache_arg_values,
                             storage_backend, storage_backend_kwargs, write_to_cache, write_to_local_cache):
    """Instantiate the write caches."""
    write_caches = {}
    for location in ['local', 'root']:
        if location == 'root' and not write_to_cache:
            # If we're not writing to the root cache, skip it
            continue

        if location == 'local' and not write_to_local_cache:
            # If we're not writing to the local cache, skip it
            continue

        key = cache_key
        if location == 'local':
            # use the memoizer cache key for the local cache to capture the full set of arguments
            key = memoizer_cache_key

        write_cache_config = config[location]
        if write_cache_config and 'filesystem' in write_cache_config:
            # Instantiate the write cache
            write_caches[location] = Cache(write_cache_config, key, namespace, version,
                                           cache_arg_values, storage_backend, storage_backend_kwargs)
            if not write_caches[location].backend:
                raise RuntimeError(
                    f"Failed to create a write cache for the requested backend {storage_backend} at location {location}. Perhaps the backend is misconfigured?")

    if 'local' in write_caches:
        # Ensure that the local cache is the last in the ordered dictionary of write caches
        local_write_cache = write_caches.pop('local')
        write_caches = {**write_caches, 'local': local_write_cache}

    return write_caches


def get_cache_mode(cache_mode):
    """Based on the configurable cache mode, return the appropriate cache read and write behavior."""
    # Set cache read and write behavior based on the write mode
    if cache_mode == 'write':  # only write to the root cache
        force_overwrite = False
        fail_if_no_cache = False
        write_global = True
        write_local = False
        read_global = True
        read_local = False
    elif cache_mode == 'overwrite':  # overwrite the root cache
        force_overwrite = True
        fail_if_no_cache = False
        write_global = True
        write_local = False
        read_global = True
        read_local = False
    elif cache_mode == 'local':  # write to the local cache
        force_overwrite = False
        fail_if_no_cache = False
        write_global = False
        write_local = True
        read_global = True
        read_local = True
    elif cache_mode == 'local_overwrite':  # overwrite the local cache
        force_overwrite = True
        fail_if_no_cache = False
        write_global = False
        write_local = True
        read_global = True
        read_local = True
    elif cache_mode == 'local_sync':  # sync the local cache to the root cache
        force_overwrite = True
        fail_if_no_cache = False
        write_global = False
        write_local = True
        read_global = True
        read_local = False
    elif cache_mode == 'local_strict':  # ignore the root cache for both read and write
        force_overwrite = False
        fail_if_no_cache = False
        write_global = False
        write_local = True
        read_global = False
        read_local = True
    elif cache_mode == 'local_api':  # act like an API - fetch globally and store locally. don't ever compute
        force_overwrite = False
        fail_if_no_cache = True
        write_global = False
        write_local = True
        read_global = True
        read_local = True
    elif cache_mode == 'read_only':  # read only from caches, no writing
        force_overwrite = False
        fail_if_no_cache = False
        write_global = False
        write_local = False
        read_global = True
        read_local = True
    elif cache_mode == 'read_only_strict':  # read only from caches, no writing, fail if no cache
        force_overwrite = False
        fail_if_no_cache = True
        write_global = False
        write_local = False
        read_global = True
        read_local = True
    elif cache_mode == 'off':  # completely disable caching
        force_overwrite = False
        fail_if_no_cache = False
        write_global = False
        write_local = False
        read_global = False
        read_local = False
    else:
        raise ValueError(
            f"Invalid cache mode: {cache_mode}. Should be one of: write, overwrite, local, local_overwrite, rootless, read_only, read_only_strict, off.")

    return write_global, write_local, read_global, read_local, force_overwrite, fail_if_no_cache


def pretty_cache_print(cache_print, memeoizer_cache_print, location):
    """Pretty print the cache dynamically based on the location."""
    if location == 'root':
        return cache_print
    else:
        return memeoizer_cache_print


def cache(cache=True,
          namespace=None,
          version=None,
          cache_args=[],
          cache_disable_if=None,
          fail_if_no_cache=False,
          engine=None,
          backend=None,
          backend_kwargs={},
          storage_backend=None,
          storage_backend_kwargs={},
          memoize=False,
          upsert_keys=None):
    """Decorator for caching function results.

    Args:
        namespace(str): The namespace in which to store the cache
        vesion(str): A cache version tag. Useful for invalidating old results on change.
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
        backend_kwargs(dict): A dictionary of backend-specific arguments that will be passed to
            and used back the backend for reading and possibly writing
        storage_backend(str): The name of the backend to use for cache storage only. None
            to match backend. Useful for pulling from one backend and writing to another.
        storage_backend_kwargs(dict): A dictionary of backend-specific arguments that will be passed to
            and used back the backend for writing
        cache_local (bool): If True, will mirror the result locally, at the location
            specified by the LOCAL_CACHE_ROOT_DIR variable. Default is False.
        memoize(bool): Whether to memoize the result in memory. Default is False.
        upsert_keys (list(str)): Column names of the primary keys to user for upsert.
    """
    # Valid configuration kwargs for the cacheable decorator
    default_cache_kwargs = {
        "namespace": None,
        "engine": None,
        "backend": None,
        "backend_kwargs": None,
        "storage_backend": None,
        "storage_backend_kwargs": None,
        "filepath_only": False,
        "recompute": False,
        "memoize": None,
        "cache_mode": None,
        "retry_null_cache": False,
        "fail_if_no_cache": False,
        "upsert": False,
        "post_processors": [],
    }
    # By resetting nonlocals to locals here we can ensure that the default
    # values of the variables for the decorater are set on every call
    nonlocals = locals()

    def create_cacheable(func):

        @wraps(func)
        def nuthatch_cacheable_wrapper(*args, **passed_kwargs):
            ###############################################################
            # 1. Handle arguments that determine the cache configuration
            ###############################################################
            final_cache_config = get_cache_args(
                passed_kwargs, default_cache_kwargs, nonlocals, func.__name__)

            # Set all the final cache config variables
            cache = final_cache_config['cache']
            cache_mode = final_cache_config['cache_mode']
            namespace = final_cache_config['namespace']
            engine = final_cache_config['engine']
            backend = final_cache_config['backend']
            backend_kwargs = final_cache_config['backend_kwargs']
            storage_backend = final_cache_config['storage_backend']
            storage_backend_kwargs = final_cache_config['storage_backend_kwargs']
            filepath_only = final_cache_config['filepath_only']
            recompute = final_cache_config['recompute']
            memoize = final_cache_config['memoize']
            retry_null_cache = final_cache_config['retry_null_cache']
            upsert = final_cache_config['upsert']
            fail_if_no_cache = final_cache_config['fail_if_no_cache']
            post_processors = final_cache_config['post_processors']
            cache_args = nonlocals['cache_args']
            cache_disable_if = nonlocals['cache_disable_if']
            upsert_keys = nonlocals['upsert_keys']
            version = nonlocals['version']

            ########################################################################################
            # 2. Calculate the cache key from the function and passed arguments (and their values)
            #######################################################################################
            # The function parameters and their values
            params = signature(func).parameters
            cache_arg_values = extract_cache_arg_values(cache_args, args, params, passed_kwargs)
            all_arg_values = extract_all_arg_values(args, params, passed_kwargs)

            # Calculate our unique cache key from the params and values
            cache_key, cache_print = get_cache_key(func, cache_arg_values)
            memoizer_cache_key, memoizer_cache_print = get_cache_key(func, all_arg_values)
            pretty_print = partial(pretty_cache_print, cache_print, memoizer_cache_print)

            ########################################################################################
            # 3. Determine cache behavior: where are we reading from, where are we writing to, etc.
            #######################################################################################
            # Disable the cache if it's enabled and the function params/values match the disable statement
            if cache:
                cache = check_cache_disable_if(cache_disable_if, cache_arg_values)

            # Get the configuration information need for cache operations (e.g., cache locations, permissions, etc.)
            config = NuthatchConfig(wrapped_module=inspect.getmodule(func))

            # If the cache mode is not set, figure out a reasonable default
            if cache_mode is None and 'root' in config:
                cache_mode = 'write'
            elif cache_mode is None:
                cache_mode = 'local_api'

            write_global, write_local, read_global, read_local, \
                force_overwrite, strict_read = get_cache_mode(cache_mode)
            # If strict read is set, overwrite fail if no cache. Otherwise, use the value from the decorator
            if strict_read:
                fail_if_no_cache = True

            if write_local and config.default_local and not check_if_nested_fn():
                # If we're using the default local cache location, warn the user in case that's unexpected
                # But only print this warning once, if we're in the top level function
                logger.warning(f"Writing to default local cache location {config['local']['filesystem']}")

            # Initialize the read caches dictionary
            read_caches = {}

            mirror_exception = None  # a boolean to track if we have failed to access a mirror cache
            if cache and read_global:
                # If the cache is enabled, we will need to read from the caches (and the mirrors, if configured.)
                # if the memoizer and local cache fail to hit
                try:
                    caches, mirror_exception = instantiate_read_caches(
                        config, cache_key, namespace, version, cache_arg_values, backend, backend_kwargs)
                except RuntimeError as e:
                    if read_global == True or write_global == True:  # noqa: E712, must check the boolean
                        raise e
                    else:
                        # If we're not writing to the global cache, we can continue
                        # This is useful for pipelines that don't need to read from the global cache
                        pass
                read_caches |= caches

            if read_local:
                # Instantiate the local cache for reading
                local_cache = instantiate_local_read_cache(
                    config, memoizer_cache_key, namespace, version, cache_arg_values, backend, backend_kwargs)
                read_caches['local'] = local_cache

                # Ensure that the local cache is the first in the ordered dictionary
                local_cache = read_caches.pop('local')
                read_caches = {'local': local_cache, **read_caches}

            ########################################################################################
            # 4. Read from the caches
            #######################################################################################
            # Initialize the dataset to None
            ds = None

            #  Boolean determining if we need to compute the result by calling the function
            compute_result = True

            # If we're not recomputing and not upserting and we have memoization enabled, try to recall from memory
            if not recompute and not upsert and memoize:
                ds = recall_from_memory(memoizer_cache_key)
                if ds:
                    logger.info(f"Found cache for {memoizer_cache_print} in memory.")
                    compute_result = False
                    return ds

            # Try to read from the read caches in the priority order already estabilished (local, root, mirrors)
            found = False
            used_read_backend = None  # the backend that was used to read the dataset
            used_read_location = None  # the location that was used to read the dataset
            # If we're not recomputing and not upserting and we didn't find the ds in memory, try to read from the caches
            if not recompute and not upsert and not ds:
                # Try to read from each of our caches in order
                # If there are not read_caches, there should be nothing in this loop
                for location, read_cache in read_caches.items():
                    # If the metadata is null this backend isn't configured - continue
                    if not read_cache:
                        continue

                    # First check if it's null
                    if read_cache.is_null():
                        logger.info(f"Found null cache for {pretty_print(location)} in {location} cache.")
                        if not retry_null_cache:
                            # We've found a null cache and we're not retrying, return None
                            return None
                        else:
                            logger.info("Retry null cache set. Recomputing.")
                            read_cache.delete_null()
                            # We've found a null cache and we're retrying, continue to the next cache
                            found = True
                            break

                    # If it's not null, see if the cache file exists
                    if read_cache.exists():
                        logger.info(
                            f"Found cache for {pretty_print(location)} with backend {read_cache.get_backend()} in {location} cache")

                        # Record the backend and location that was used to read the dataset
                        used_read_backend = read_cache.get_backend()
                        used_read_location = location
                        if filepath_only:
                            return read_cache.get_uri()
                        else:
                            ds = read_cache.read(engine=engine)
                            compute_result = False
                            found = True
                            break

                if not found and mirror_exception:
                    logger.warning(mirror_exception)

            ########################################################################################
            # 4. If we need to compute the result, do so by calling the function
            #######################################################################################
            if compute_result:
                if fail_if_no_cache and cache == True:  # noqa: E712, must check the boolean
                    # If this is a cachable function and we're in strict read mode, raise an error
                    raise RuntimeError(
                        f"Computation has been disabled by strict read mode and cache doesn't exist for {cache_print}.")
                elif recompute:
                    logger.info(f"Recompute for {cache_print} requested. Not checking for cached result.")
                elif upsert:
                    logger.info(f"Computing {cache_print} to enable data upsert.")
                elif not cache:
                    # The function isn't cacheable, recomputing
                    pass
                else:
                    if fail_if_no_cache:
                        raise RuntimeError(
                            f"Computation has been disabled by strict read mode and cache doesn't exist for {cache_print}.")

                    # If we're about to do computation, but we don't have cache configured, ask the user if they still want to compute the result
                    if (write_global and 'root' not in config) or (write_local and 'local' not in config):
                        inp = input("""You are recomputing and a cache was not found to store the result.
                                       You can fix this by adding a valid cache to your nuthatch configuration.
                                       Would you still like to compute the result? (y/n)""")
                        if inp == 'y' or inp == 'Y':
                            pass
                        else:
                            raise RuntimeError("No cache configured to store the result. Exiting.")
                    logger.info(
                        f"Cache doesn't exist for {cache_print} in namespace {namespace if namespace else 'default'}. Running function.")

                # Print a warning if the data processing is computationally intensive
                logger.info(f"Processing data by running function {func.__name__}... ")
                logger.info("Data processing can be computationally intensive. Ensure you have sufficient compute resources.")

                ##### COMPUTE THE RESULT BY CALLING THE UNDERLYING FUNCTION ######
                ds = func(*args, **passed_kwargs)
                ##########################

            ########################################################################################
            # 5. Save the computed result to memory and any other write caches
            #######################################################################################
            if memoize:
                logger.info(f"Memoizing {memoizer_cache_print} in your local memory for fast recall.")
                # Apply any post-processors to the dataset, so the filtered data is saved to memory
                filtered_ds = ds
                for processor in post_processors:
                    filtered_ds = processor(filtered_ds)
                # Save the filtered dataset to memory
                save_to_memory(memoizer_cache_key, filtered_ds, config['root'])

            # Get the appropriate storage backend for the dataset
            storage_backend, storage_backend_kwargs = get_storage_backend(
                ds, backend, backend_kwargs, storage_backend, storage_backend_kwargs)

            # Write to cache if:
            # 1. Recomputing the result
            # 2. Needing to store it in a new backend
            # 3. Needing to store it in a new location
            write_to_cache = cache and write_global and (
                compute_result or (storage_backend != used_read_backend))

            # Write to local cache if:
            # 1. We request it and
            #   we either recomputed it OR
            #   we read it from the cache (so it must not exist in local)
            # 2. We need to store it in a new backend
            # Note that we do not include the cache here: all functions are cachable when writing locally
            write_to_local_cache = (write_local and
                                    (compute_result or
                                     (used_read_location != 'local' and used_read_location is not None) or
                                        (storage_backend != used_read_backend)
                                     )
                                    )

            # Instantiate the write caches
            write_caches = instantiate_write_caches(
                config, cache_key, memoizer_cache_key, namespace, version, cache_arg_values,
                storage_backend, storage_backend_kwargs, write_to_cache, write_to_local_cache)

            # If we have no write caches and we need to write to one, warn the user
            if len(write_caches) == 0 and (write_to_cache or write_to_local_cache):
                logger.warning("No cache has been configured, so the computed object cannot be cached."
                               "Please configure a cache if you would like to cache and retrieve the output of this function.")
                return ds

            # Ensure that the result is written to the proper write cache(s)
            # Need to set outside of the loop in case you don't have a write cache
            return_value = ds
            for location, write_cache in write_caches.items():
                # If we're writing to the local cache, apply any post-processors to the dataset
                if location == 'local':
                    filtered_ds = ds
                    for processor in post_processors:
                        filtered_ds = processor(filtered_ds)
                    write_ds = filtered_ds
                else:
                    write_ds = ds

                if write_ds is None:
                    # If, before / after filtering, the final dataest is none, write the cache to null
                    if not upsert:
                        write_cache.set_null()
                    else:
                        logger.info("Null result not cached in upsert mode.")

                    continue

                if write_cache.exists() and not upsert and ((write_local and location == 'local') or (write_global and location == 'root')):
                    # If the cache exists and we would need to write to it / overwrite it
                    if not force_overwrite:
                        # Ask the user
                        inp = input(f"""A cache already exists at {pretty_print(location)} for type {write_cache.get_backend()} in {location} cache.
                                    Are you sure you want to overwrite it? (y/n)""")
                        write = True if inp == 'y' or inp == 'Y' else False
                    else:
                        logger.info(f"Overwriting existing cache for {pretty_print(location)} in {location} cache.")
                        write = True
                else:
                    write = True

                # If we have read value, return that rather
                # This will get set to the last cache written, which is by default the local cache
                return_value = write_ds
                if write:
                    logger.info(
                        f"Caching result for {pretty_print(location)} in {write_cache.get_backend()} with namespace {namespace if namespace else 'default'} in {location} cache.")
                    if upsert:
                        return_value = write_cache.upsert(write_ds, upsert_keys=upsert_keys)
                    else:
                        return_value = write_cache.write(write_ds)

                if filepath_only:
                    # If we only need to return the filepath, return it
                    return_value = write_cache.get_uri()

            for processor in post_processors:
                return_value = processor(return_value)
            return return_value

        # Set a custom attribute to mark this as a cacheable function
        setattr(nuthatch_cacheable_wrapper, '__nuthatch_cacheable__', True)
        return nuthatch_cacheable_wrapper

    return create_cacheable
