"""The main module for the nuthatch package.

This module contains the main decorator for caching functions and the global
variables for caching configuration.
"""
from functools import wraps
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
global_force_overwrite = None
global_retry_null_cache = None
global_fs_warning = []

def set_global_cache_variables(recompute=None, memoize=None, force_overwrite=None,
                               retry_null_cache=None):
    """Reset all global variables to defaults and set the new values."""
    global global_recompute, global_memoize, global_force_overwrite, \
           global_retry_null_cache

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
    # skip the first two frames (this function and the current cacheable function)
    for frame_info in stack[3:]:
        frame = frame_info.frame
        func_name = frame.f_code.co_name
        if func_name == "cacheable_wrapper":
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

    if cache_args['cache'] is None:
        cache_args['cache'] = decorator_args['cache']
    if cache_args['namespace'] is None:
        cache_args['namespace'] = decorator_args['namespace']
    if cache_args['engine'] is None:
        cache_args['engine'] = decorator_args['engine']
    if cache_args['memoize'] is None:
        cache_args['memoize'] = decorator_args['memoize']
    if cache_args['backend'] is None:
        cache_args['backend'] = decorator_args['backend']

    if 'backend_kwargs' in cache_args and isinstance(cache_args['backend_kwargs'], dict):
        cache_args['backend_kwargs'] = cache_args['backend_kwargs'].update(decorator_args['backend_kwargs'])
    elif 'backend_kwargs' in cache_args and cache_args['backend_kwargs'] is None:
        cache_args['backend_kwargs'] = decorator_args['backend_kwargs']

    if cache_args['cache_local'] is None:
        cache_args['cache_local'] = decorator_args['cache_local']

    # Check if this is a nested cacheable function
    if not check_if_nested_fn():
        # This is a top level cacheable function, reset global cache variables
        set_global_cache_variables(recompute=cache_args['recompute'], memoize=cache_args['memoize'],
                                   force_overwrite=cache_args['force_overwrite'],
                                   retry_null_cache=cache_args['retry_null_cache'])
        if isinstance(cache_args['recompute'], list) or isinstance(cache_args['recompute'], str) or cache_args['recompute'] == '_all':
            cache_args['recompute'] = True
        if isinstance(cache_args['memoize'], list) or isinstance(cache_args['memoize'], str) or cache_args['memoize'] == '_all':
            cache_args['memoize'] = True
    else:
        # Inherit global cache variables
        global global_recompute, global_memoize, global_force_overwrite, global_retry_null_cache

        # Set all global variables
        if global_force_overwrite is not None:
            cache_args['force_overwrite'] = global_force_overwrite
        if global_retry_null_cache is not None:
            cache_args['retry_null_cache'] = global_retry_null_cache
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
        A priority ordered list of caches to read from.
    """
    # The general order of priority to check validity is:
    # (1) local if local is requested and local config is provided
    # (2) the root cache
    # (3) any mirror caches

    # Start by trying to instantiate the metadata stores
    # then do our best to instantiate the backends themselves
    caches = {}
    found_cache = False

    mirror_exception = None
    global global_fs_warning
    for location, location_values in config.items():
        cache = None
        if location.startswith('mirror'):
            if 'filesystem' in location_values:
                if 'skipped_filesystems' in config['root'] and location_values['filesystem'] in config['root']['skipped_filesystems']:
                    if location_values['filesystem'] not in global_fs_warning:
                        logger.warn(f"Skipping filesystem {location_values['filesystem']} because it has been added to the excluded filesystems list in ~/.nuthatch.toml. Please remove it if you now have access.")
                        global_fs_warning.append(location_values['filesystem'])
                    continue

                try:
                    cache = Cache(location_values, cache_key, namespace, version, cache_arg_values, requested_backend, backend_kwargs)
                    caches[f"{location}"] = cache
                    found_cache=True
                except Exception as e: # noqa
                    logger.warn(f"Failed to access the cache at {location_values['filesystem']}. Adding it to the excluded filesystems list at ~/.nuthatch.toml so future runs are faster. Please remove it if you gain access in the future.")
                    set_global_skipped_filesystem(location_values['filesystem'])
                    global_fs_warning.append(location_values['filesystem'])
                    mirror_exception = f'Failed to access configured nuthatch mirror "{location}" with error "{type(e).__name__}: {e}". If you couldn`t access the expected data, this could be the reason.'
        else:
            if 'filesystem' in location_values:
                try:
                    cache = Cache(location_values, cache_key, namespace, version, cache_arg_values, requested_backend, backend_kwargs)
                    caches[location] = cache
                    found_cache=True
                except Exception as e:
                    raise RuntimeError(f'Nuthatch is unable to access the configured root cache at {location_values["filesystem"]} with error "{type(e).__name__}: {e}." If you configure a root cache it must be accessible. Please ensure that you have correct access credentials for the configured root cache.')

    if not found_cache:
        raise RuntimeError("""No Nuthatch configuration has been found globally, in the current project, or in the module you have called.
                                    -> If you are developing a Nuthatch project, please configure nuthatch in your pyproject.toml
                                    -> If you are calling a project that uses nuthatch, it should just work! Please contact the project's maintainer
                           """)

    return (caches, mirror_exception)


def cache(cache=True,
          namespace=None,
          version=None,
          cache_args=[],
          cache_disable_if=None,
          engine=None,
          backend=None,
          backend_kwargs={},
          storage_backend=None,
          storage_backend_kwargs={},
          cache_local=False,
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
        "cache": None,
        "namespace": None,
        "engine": None,
        "backend": None,
        "backend_kwargs": None,
        "cache_local": False,
        "storage_backend": None,
        "storage_backend_kwargs": None,
        "filepath_only": False,
        "recompute": False,
        "memoize": False,
        "force_overwrite": None,
        "retry_null_cache": False,
        "upsert": False,
        "fail_if_no_cache": False,
    }


    # By resetting nonlocals to locals here we can ensure that the default
    # values of the variables for the decorate are set on every call
    nonlocals = locals()

    def create_cacheable(func):

        @wraps(func)
        def cacheable_wrapper(*args, **passed_kwargs):
            final_cache_config = get_cache_args(passed_kwargs, default_cache_kwargs, nonlocals, func.__name__)

            # Set all the final cache config variables
            cache = final_cache_config['cache']
            namespace = final_cache_config['namespace']
            engine = final_cache_config['engine']
            backend = final_cache_config['backend']
            backend_kwargs = final_cache_config['backend_kwargs']
            storage_backend = final_cache_config['storage_backend']
            storage_backend_kwargs = final_cache_config['storage_backend_kwargs']
            filepath_only = final_cache_config['filepath_only']
            recompute = final_cache_config['recompute']
            cache_local = final_cache_config['cache_local']
            memoize = final_cache_config['memoize']
            force_overwrite = final_cache_config['force_overwrite']
            retry_null_cache = final_cache_config['retry_null_cache']
            upsert = final_cache_config['upsert']
            fail_if_no_cache = final_cache_config['fail_if_no_cache']
            cache_args = nonlocals['cache_args']
            cache_disable_if = nonlocals['cache_disable_if']
            upsert_keys = nonlocals['upsert_keys']
            version = nonlocals['version']

            # The function parameters and their values
            params = signature(func).parameters
            cache_arg_values = extract_cache_arg_values(cache_args, args, params, passed_kwargs)
            all_arg_values = extract_all_arg_values(args, params, passed_kwargs)

            # Disable the cache if it's enabled and the function params/values match the disable statement
            if cache:
                cache = check_cache_disable_if(cache_disable_if, cache_arg_values)

            # Calculate our unique cache key from the params and values
            cache_key = get_cache_key(func, cache_arg_values)
            memoizer_cache_key = get_cache_key(func, all_arg_values)

            ds = None
            compute_result = True

            config = NuthatchConfig(wrapped_module=inspect.getmodule(func))
            read_caches, mirror_exception = instantiate_read_caches(config, cache_key, namespace, version, cache_arg_values, backend, backend_kwargs)

            # Try to sync local/remote only once on read. All syncing is done lazily
            if cache_local:
                if not read_caches['local']:
                    raise ValueError("Local filesystem must be configured if local caching is requested.")

                read_caches['local'].sync(read_caches['root'])
            else:
                # If local isn't set we shouldn't use it even if it's configured
                read_caches['local'] = None


            # Try the memoizer first
            if not recompute and not upsert and (cache or memoize):
                ds = recall_from_memory(memoizer_cache_key)
                if ds:
                    logger.info(f"Found cache for {memoizer_cache_key} in memory.")
                    compute_result = False

            # Try to read from the cache in priority locations
            used_read_backend = None
            found = False
            if not recompute and not upsert and cache and not ds:
                for location, read_cache in read_caches.items():
                    # If the metadata is null this backend isn't configured - continue
                    if not read_cache:
                        continue

                    # First check if it's null
                    if read_cache.is_null():
                        logger.info(f"Found null cache for {cache_key} in {location} cache.")
                        if retry_null_cache:
                            logger.info("Retry null cache set. Recomputing.")
                            read_cache.delete_null()
                            found = True
                            break
                        else:
                            return None

                    # If it's not null see if it exists
                    if read_cache.exists():
                        logger.info(f"Found cache for {cache_key} with backend {read_cache.get_backend()} in {location} cache")

                        used_read_backend = read_cache.get_backend()
                        if filepath_only:
                            return read_cache.get_uri()
                        else:
                            ds = read_cache.read(engine=engine)

                            if memoize:
                                logger.info(f"Memoizing {memoizer_cache_key}.")
                                save_to_memory(memoizer_cache_key, ds, config['root'])

                            compute_result = False
                            found = True
                            break

                if not found and mirror_exception:
                    logger.warning(mirror_exception)

            # If the cache doesn't exist or we are recomputing, compute the result
            if compute_result:
                if recompute:
                    logger.info(f"Recompute for {cache_key} requested. Not checking for cached result.")
                elif upsert:
                    logger.info(f"Computing {cache_key} to enable data upsert.")
                elif not cache:
                    # The function isn't cacheable, recomputing
                    pass
                else:
                    if fail_if_no_cache:
                        raise RuntimeError(f"""Computation has been disabled by
                                            `fail_if_no_cache` and cache doesn't exist for {cache_key}.""")

                    if 'root' not in config:
                        logger.info(f"Module {inspect.getmodule(func)}")
                        inp = input("""A pre-existing cache was not found, and no root cache has been configured.
                                        Would you still like to compute the result? (y/n)""")
                        if inp == 'y' or inp == 'Y':
                            pass
                        else:
                            raise RuntimeError("Root cache not configured to store cache output. Exiting.")

                    if namespace:
                        logger.info(f"Cache doesn't exist for {cache_key} in namespace {namespace}. Running function")
                    else:
                        logger.info(f"Cache doesn't exist for {cache_key} in default namespace. Running function")

                ##### IF NOT EXISTS ######
                ds = func(*args, **passed_kwargs)
                ##########################

                if memoize:
                    logger.info(f"Memoizing {memoizer_cache_key}.")
                    save_to_memory(memoizer_cache_key, ds, config['root'])


            # Store the result
            if cache and (compute_result or (storage_backend and storage_backend != used_read_backend)):
                # Instantiate write backend
                write_cache = None
                write_cache_config = config['root']
                if write_cache_config and 'filesystem' in write_cache_config:
                    if not storage_backend and not backend:
                        storage_backend = get_default_backend(type(ds))
                        if not storage_backend_kwargs:
                            storage_backend_kwargs = backend_kwargs
                    elif backend:
                        storage_backend = backend
                        storage_backend_kwargs = backend_kwargs

                    write_cache = Cache(write_cache_config, cache_key, namespace, version,
                                        cache_arg_values, storage_backend, storage_backend_kwargs)
                    if not write_cache.backend:
                        raise RuntimeError(f"Failed to create a write cache for the requested backend {storage_backend}. Perhaps the backend is misconfigured?")
                else:
                    #raise ValueError("At least a root filesystem for metadata storage must be configured. No configuration found.")
                    logger.warn("No root cache has been configured, so the computed object cannot be cached. Please configure a root cache if you would like to cache and retrieve the output of this function.")
                    return ds


                if ds is None:
                    if not upsert:
                        write_cache.set_null()
                    else:
                        logger.info("Null result not cached in upsert mode.")

                    return None

                write = False  # boolean to determine if we should write to the cache
                if (write_cache.exists() and force_overwrite is None and not upsert):
                    inp = input(f"""A cache already exists at {cache_key} for type {write_cache.get_backend()}
                                Are you sure you want to overwrite it? (y/n)""")
                    if inp == 'y' or inp == 'Y':
                        write = True
                elif force_overwrite is False:
                    pass
                else:
                    write = True

                if write:
                    if namespace:
                        logger.info(f"Caching result for {cache_key} in {write_cache.get_backend()} in namespace {namespace}.")
                    else:
                        logger.info(f"Caching result for {cache_key} in {write_cache.get_backend()} in default namespace.")

                    if upsert:
                        ds = write_cache.upsert(ds, upsert_keys=upsert_keys)
                    else:
                        ds = write_cache.write(ds)

            if filepath_only:
                return write_cache.get_uri()
            else:
                return ds

        return cacheable_wrapper
    return create_cacheable
