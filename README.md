# Nuthatch

[![Tests](https://github.com/rhiza-research/nuthatch/actions/workflows/test.yml/badge.svg)](https://github.com/rhiza-research/nuthatch/actions/workflows/test.yml)

Nuthatch is a tool for building pure-python big data pipelines. At its core it
enables the transparent multi-level caching and recall of results in formats that 
are efficient for each data type. It supports a variety of 
common storage backends, data processing frameworks, and their associated
data types for caching. 

It also provides a framework for re-using and sharing data-type specific
post-processing, and for these data type
processors to pass hints to storage backends for more efficient storage and recall.

Nuthatch was created to alleviate the comon pattern of data processing pipelines manually
specifying their output storage locations, and the requirements of pipeline builders to
use external data orchestration tools to specify the execution of their pipeliness. With Nuthatch
simply tag your functions and anyone who has access to your storage backend - you, your
team, or the public - can acess and build off of your most up-to-date data.

## Using Nuthatch

### Configuration

To use Nuthatch you must configure access to some file store. Most basically this could be your local
filesystem, but is likely more useful if it's a remote cloud bucket (like gcs, s3, etc). Configuration
is done in your pyproject.toml, e.g.

```toml
[tool.nuthatch]
filesystem = "s3://my-bucket/caches"

[tool.nuthatch.filesystem_options]
key = "your_key_id"
secret= "your_secret_key"
```

This is sufficient to enable nuthatch to store data in all file-like backends. Other backends, like databases,
will require additional configuration parameters

### Dynamic Secrets

You *should not* save secrets in pyproject.toml. To solve this problem nuthatch enables a method of fetching
secrets dynamically, from a cloud secret store, or from another location like an environment variable or
file. Just make sure this file is imported before you run your code

```python
from nuthatch import config_parameter

@config_parameter('filesystem_options', secret=True)
def fetch_key():
    # Fetch from secret store, environment, etc
    filesystem_options = {
        'key': os.environ['S3_KEY'],
        'secret': os.environ['S3_SECRET']
    }

    return filesystem_options
```

### Your first cache

Now you can make your first cache! Simple tag with nuthatch, and nuthatch will do its best to store
your data efficiently.

```python
from nuthatch import cache
from nuthatch.processors import timeseries
import xarray as xr

@cache()
def my_first_cache():
    ds = xr.tutorial.open_dataset("air_temperature")

    # Data will automatically be saved in a zarr store and recalled
    return ds

my_first_cache()
```

Nuthatch has many more features too:
 - Caches that are keyed by argument
 - Processors to enable slicing and data validation 
 - Rerunning of DAGs explicitly
 - Per-data-type memoization of results (i.e. persisting an xarray and recalling the compute graph from memory)
 - Caching of data locally for lower-latency access
 - Namespacing of caches to rerun the same data pipeline for multiple scenarios
 
```python
@timeseries(timeseries='time')
@cache(cache_args=['agg_days'])
def agg_and_clip(start_time, end_time, agg_days=1):
    ds = my_first_cache()

    # aggregate based on time
    ds = ds.rolling({'time': agg_days}).mean()

    return ds

# Daily aggregate
agg_and_clip("2013-01-01", "2014-01-01", agg_days=1)

# Daily aggregate recalled, persisted in memory, and clipped to 2013-06
agg_and_clip("2013-01-01", "2013-06-01", agg_days=1, memoize=True) 

# Daily aggregate recalled from memory and clipped to 2013-06
agg_and_clip("2013-01-01", "2013-06-01", agg_days=1, memoize=True) 

# Weekly aggregate computed fresh
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7)

# Weekly aggregate recomputed and overwrite existing cache
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=True, force_overwrite=True)

# Weekly aggregate with both functions recomputed and overwritten
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=['agg_and_clip', 'my_first_cache'], force_overwrite=True) 
```

## Nuthatch Limitations

Current limitations:
 - Arguments must be basic types, not objects to key caches
 - There is currently no mechansim to detect cache "staleness". Automatically tracking and detecting changes is planned for future work.
 - Expanded configurability (i.e. directly from environment variable) is not supported
 - Nuthatch's metastore (i.e. the database that tracks the caches and their versions) is still in flux. It adds a couple of seconds of write overhead for writing. Future work will try to eliminate this.
