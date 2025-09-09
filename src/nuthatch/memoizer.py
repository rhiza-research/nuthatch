"""The memoizer is a module that memoizes data in memory."""
import sys
import copy
import xarray as xr
import dask.dataframe as dd
from .config import get_config

import logging
logger = logging.getLogger(__name__)

memoized_objects = {}
cache_key_lru = []

def save_to_memory(cache_key, data):
    """Save data to memory.

    Implements special handling for xarray and dask dataframes.
    Evicts the least recently used object when the memory limit is reached.

    Args:
        cache_key (str): The key to save the data to.
        data (any): The data to save to memory.
    """
    if isinstance(data, xr.Dataset):
        data = data.persist()
    elif isinstance(data, dd.DataFrame):
        data = data.persist()
    else:
        pass

    max_size = get_config(location='root', requested_parameters=['maximum_memory_usage'])
    if 'maximum_memory_usage' in max_size:
        max_size = max_size['maximum_memory_usage']
    else:
        # 100MB Default
        max_size = 100*10^6

    if(sys.getsizeof(data) > max_size):
        logger.warning("WARNING: Data too large to memoize.")
        return

    while(sys.getsizeof(memoized_objects) + sys.getsizeof(data) > max_size):
        del memoized_objects[cache_key_lru[0]]
        del cache_key_lru[0]

    memoized_objects[cache_key] = copy.deepcopy(data)

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
    if cache_key in memoized_objects:
        # refresh the lru
        cache_key_lru.remove(cache_key)
        cache_key_lru.append(cache_key)

        # return the object
        return memoized_objects[cache_key]
    else:
        return None
