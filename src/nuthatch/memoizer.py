"""The memoizer is a module that memoizes data in memory."""
import sys
import copy
import xarray as xr
import dask.dataframe as dd

import logging
logger = logging.getLogger(__name__)

local_memoized_objects = {}
local_object_size = {}
local_cache_key_lru = []
remote_memoized_objects = {}
remote_object_size = {}
remote_cache_key_lru = []

def get_cache_usage(object_sizes):
    return sum(object_sizes.values())

def save_to_memory(cache_key, data, config):
    """Save data to memory.

    Implements special handling for xarray and dask dataframes.
    Evicts the least recently used object when the memory limit is reached.

    Args:
        cache_key (str): The key to save the data to.
        data (any): The data to save to memory.
    """
    remote = False
    if isinstance(data, xr.Dataset) or isinstance(data, xr.DataArray):
        data = data.persist()
        data_size = data.nbytes
        remote=True
    elif isinstance(data, dd.DataFrame):
        data = data.persist()
        data_size = int(data.memory_usage().sum())
        remote=True
    else:
        data_size = sys.getsizeof(data)


    if remote:
        memoized_objects = remote_memoized_objects
        object_size = remote_object_size
        cache_key_lru = remote_cache_key_lru
        if 'remote_cache_size' in config:
            max_size = config['remote_cache_size']
        else:
            max_size = 32*(10**9)
    else:
        memoized_objects = local_memoized_objects
        object_size = local_object_size
        cache_key_lru = local_cache_key_lru
        if 'local_cache_size' in config:
            max_size = config['local_cache_size']
        else:
            max_size = 2*(10**9)

    if(data_size > max_size):
        logger.warning("WARNING: Data too large to memoize.")
        return

    while((get_cache_usage(object_size) + data_size) > max_size):
        if len(cache_key_lru) == 0:
            logger.error("ERROR: Nuthatch memoizer size management mismatch. Memoization will no longer function.")
        else:
            del memoized_objects[cache_key_lru[0]]
            del object_size[cache_key_lru[0]]
            del cache_key_lru[0]

    memoized_objects[cache_key] = copy.deepcopy(data)
    object_size[cache_key] = data_size

    if cache_key in cache_key_lru:
        cache_key_lru.remove(cache_key)

    cache_key_lru.append(cache_key)


def recall_from_memory(cache_key):
    """Recall data from memory.

    Refreshes the LRU cache when the data is recalled.

    Args:
        cache_key (str): The key to recall the data from.

    Returns:
        The memoized object.
    """
    if cache_key in local_memoized_objects:
        # refresh the lru
        local_cache_key_lru.remove(cache_key)
        local_cache_key_lru.append(cache_key)

        # return the object
        return local_memoized_objects[cache_key]
    if cache_key in remote_memoized_objects:
        # refresh the lru
        remote_cache_key_lru.remove(cache_key)
        remote_cache_key_lru.append(cache_key)

        # return the object
        return remote_memoized_objects[cache_key]

    else:
        return None
