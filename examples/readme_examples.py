from nuthatch import cache
from nuthatch.processors import timeseries
import xarray as xr

@cache()
def my_first_cache():
    ds = xr.tutorial.open_dataset("air_temperature")

    # Data will automatically be saved in a zarr store and recalled
    return ds



@timeseries(timeseries='time')
@cache(cache_args=['agg_days'])
def agg_and_clip(start_time, end_time, agg_days=1):
    ds = my_first_cache()

    # aggregate based on time
    ds = ds.rolling({'time': agg_days}).mean()

    return ds

my_first_cache()
agg_and_clip("2013-01-01", "2014-01-01", agg_days=1) # Daily aggregate
agg_and_clip("2013-01-01", "2013-06-01", agg_days=1, memoize=True) # Dailu aggregate recalled, persisted in memory, and clipped to 2021
agg_and_clip("2013-01-01", "2013-06-01", agg_days=1, memoize=True) # Daily aggregate recalled from memory and clipped to 2021
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7) # Weekly aggregate computed fresh
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=True, force_overwrite=True) # Weekly aggregate recomputed and overwrite existing cache
agg_and_clip("2013-01-01", "2014-01-01", agg_days=7, recompute=['agg_and_clip', 'my_first_cache'], force_overwrite=True) # Weekly aggregate with both functions recomputed and overwritten

