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

## Getting started

The most basic form of Nuthatch simply stores and recalls your data based on its arguments in efficient formats:

```python
from nuthatch import cache
import xarray as xr

@cache()
def my_first_cache():
    ds = xr.tutorial.open_dataset("air_temperature")

    # Data will automatically be saved in a zarr store and recalled
    return ds

my_first_cache()
```

But it's much more powerful if you configure nuthatch to be shared across a team:

```python
from nuthatch import cache, set_parameter
import xarray as xr

set_parameter({'filesystem': "gs://my-datalake"})

@cache()
def my_first_cache():
    ds = xr.tutorial.open_dataset("air_temperature")

    # Data will automatically be saved in a zarr store and recalled
    return ds

my_first_cache()
```

Commit your code and anyone with access to your datalake has access to a self-documented cache of your data.

More powerful - push your code to pypi and anyone who imports your code can access the data simply
by calling the function (assuming they have read-only access to the storage.)

## Nuthatch configuration

To use Nuthatch you must configure access to some file store. Most basically this could be your local
filesystem, but is likely more useful if it's a remote cloud bucket (like gcs, s3, etc). Configuration
can be done in three places: (1) in your pyproject.toml, (2) in a special nuthatch.toml built into your package
or (3) in your code - useful if you need to access secrets dynamical or configure nuthatch on distributed workers.

Nuthatch itself and most storage backends only need access to a filesystem. Some storage backends, like databases,
may need additional parameters.

Nuthatch also has different storage locations. The ``root`` location is where things are stored by default, but
users can also configure ``local`` locations for faster data access and ``mirror`` locations that are read only data
sources. Imported projects from other python modules are automatically set up as ``mirror`` locations.

### TOML Configuration

In either pyproject.toml or src/nuthatch.toml:

```toml
[tool.nuthatch]
filesystem = "s3://my-bucket/caches"

[tool.nuthatch.filesystem_options]
key = "your_key_id"
secret= "your_secret_key"
```

pyproject.toml cannot be easily packaged. If you would like your caches to be accessible
when your package is installed and imported by others, you must use either a nuthatch.toml
file or dynamic configuration.

### Dynamic configuration - decorators

You *should not* save secrets in files. To solve this problem nuthatch enables a method of fetching
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

### Dynamic configuration - direct setting

You can also simply set configuration parameters in code, which is sometimes necessary
for distributed environments

```python
from nuthatch import set_parameter
set_parameter({'filesystem': "gs://my-datalake"})
```

### Backend-specific configuration

Nuthatch backends can be individually configured - for instance if all of your Zarr's are too big for
the datalake and need cheaper storage you can set the zarr backend to have a different fileysystem location:

```toml
[tool.nuthatch.root.zarr]
filesystem = "s3://my-zarr-bucket/"
```

## More advanced caching

Nuthatch has many more features:
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
