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
use external data orchestration tools to specify the execution of their pipelines. With Nuthatch
simply tag your functions and anyone who has access to your storage backend - you, your
team, or the public - can access and build off of your most up-to-date data.

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

my_first_cache(cache_mode='local')
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

## Slightly more advanced use cases

Nuthatch has many more features:
 - Caches that are keyed by argument
 - Processors to enable slicing and data validation 
 - Rerunning of DAGs explicitly
 - Per-data-type memoization of results (i.e. persisting an xarray and recalling the compute graph from memory)
 - Caching of data locally for lower-latency access
 - Namespacing of caches to rerun the same data pipeline for multiple scenarios
 - Cache versioning (to invalidate stale caches)
 
```python
set_parameter({'filesystem': "gs://my-datalake"})

@timeseries(timeseries='time')
@cache(cache_args=['agg_days'],
       version="0.1.0")
def agg_and_clip(start_time, end_time, agg_days=1):
    ds = my_first_cache()

    # aggregate based on time
    ds = ds.rolling({'time': agg_days}).mean()

    return ds

# Daily aggregate
agg_and_clip("2013-01-01", "2014-01-01", agg_days=1)

# Daily aggregate recalled, persisted in memory (or cluster memory if setup), and clipped to 2013-06
agg_and_clip("2013-01-01", "2013-06-01", agg_days=1, memoize=True) 

# Daily aggregate recalled from memory and clipped to 2013-06
agg_and_clip("2013-01-01", "2013-06-01", agg_days=1, memoize=True) 

# Weekly aggregate computed fresh
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7)

# Weekly aggregate recomputed and overwrite existing cache
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=True, cache_mode='overwrite')

# Weekly aggregate with both functions recomputed and overwritten
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=['agg_and_clip', 'my_first_cache'], cache_mode='overwrite') 

# Weekly aggregate with both functions recomputed and saved to a local cache for faster recall
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=['agg_and_clip', 'my_first_cache'], cache_mode='local') 

# Weekly aggregate with cache saved to a new namespace
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, namespace='experiment2')
```

## Nuthatch caching levels

### Root
The root cache is your main storage location. It's often the remote cloud bucket serving as your project's datalake, but it could also be a shared drive on a local cluster.

### Local
If you use a local mode, nuthatch will automatically create a local cache for you and store data there for more efficient recall.

### Mirror(s)
You can configure any number of read-only mirrors to look for your data in. If you import a project that uses nuthatch its root and all of its mirrors will be added as your mirrors so that you can fetch nuthatch data/functions that it defines.

## Nuthatch cache modes

When calling nuthatch functions you can operate in several distinct modes which control which of the levels you write to and read from.

#### cache_mode='write'
The default mode when you have a root cache configured. By default writes to and reads from the root cache if the function is set to be cached. This mode prompts the user if a cache exists before overwriting.

#### cache_mode='overwrite'
Same as above but does not prompt the user before overwriting.

#### cache_mode='read_only'
This reads from all available cache locations but writes to no location, simply returns the results (or computes them if they do not exist). This is the default mode if you do not have a root configured. This still allows you to import external projects and read their data.

#### cache_mode='local'
This mode reads from all available caches, and will store results to your local cache for faster recall. This is useful for the faster recall of data from a remote source.

#### cache_mode='offline'
This mode only reads from local caches and doesn't read from the root cache.

## Nuthatch supported backends

Nuthatch supports multiple backends for writing, and multiple engines (datatypes) for reading from those backends. The following are currently supported. Backends beyond the defaults can be configured by passing `backend='name'` and read data types beyond the default can be configured by passing `engine=<type>`. I.e. a function returning pandas dataframe for storing in parquet instead of delta could pass `backend='parquet', engine=pandas.DataFrame`

| Backend | Default for Data Type | Parameters | Supported read engines |
|---|:---:|:---:|:---:|
| Basic (pickle) | Basic python types, default for unmatched types | filesystem, <br>filesystem_options (optional) | N/A, unpickles to stored typed |
| Zarr | xarray.Dataset, xarray.DataArray | filesystem, <br>filesystem_options (optional) | xarray.Dataset, xarray.DataArray |
| Parquet | dask.dataframe.DataFrame | filesystem, <br>filesystem_options (optional) | pandas.DataFrame, dask.dataframe.DataFrame |
| Deltalake | pandas.DataFrame | filesystem, <br>filesystem_options (optional) | pandas.DataFrame, dask.dataframe |
| SQL | None | host, port, database, driver, user, password,<br>write_user (optional), write_password (optional) | pandas, dask.dataframe |




## Nuthatch configuration

Create a `nuthatch.toml` file in your project root:

```toml
[root]
filesystem = "gs://my-bucket/caches"

[local]
filesystem = "~/.nuthatch/local-cache"
```

Or set configuration in code:

```python
from nuthatch import set_parameter
set_parameter({'filesystem': "gs://my-datalake"})
```

Nuthatch also supports environment variable overrides, dynamic credential
fetching via `@config_parameter`, backend-specific configuration,
multi-project config merging, and more.

If you plan to distribute your project as a package (e.g. on PyPI), additional
build configuration is needed so that `nuthatch.toml` is included in the
installed package. See the [full configuration guide](docs/CONFIG.md) for
packaging instructions and advanced usage.


## Nuthatch Limitations

Current limitations:
 - Arguments must be basic types, not objects to key caches
 - There is currently no mechanism to detect cache "staleness". Automatically tracking and detecting changes is planned for future work.
