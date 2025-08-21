from nuthatch.beckend import FileBackend, register_backend
import datetime
import dask.dataframe as dd
import pandas as pd


def write_parquet_helper(df, path, partition_on=None):
    """Helper to write parquets."""
    df.to_parquet(
        path,
        overwrite=True,
        partition_on=partition_on,
        engine="pyarrow",
        write_metadata_file=True,
        write_index=False,
    )


def write_to_parquet(df, cache_path, temp_cache_path, upsert=False, primary_keys=None):
    """Write a pandas or dask dataframe to a parquet."""
    part = None
    if hasattr(df, 'cache_partition'):
        part = df.cache_partition

    fs = fsspec.core.url_to_fs(cache_path, **CACHE_STORAGE_OPTIONS)[0]

    if upsert and self.fs.exists(cache_path):
        print("Found existing cache for upsert.")
        if primary_keys is None:
            raise ValueError("Upsert may only be performed with primary keys specified")

        if isinstance(df, pd.DataFrame):
            print("Auto converting pandas to dask dataframe.")
            df = dd.from_pandas(df)

        if not isinstance(df, dd.DataFrame):
            raise RuntimeError("Upsert is only supported by dask dataframes for parquet")

        existing_df = read_from_parquet(cache_path)

        # Record starting partitions
        start_parts = df.npartitions
        existing_parts = existing_df.npartitions

        # Coearce dtypes before joining
        for key in primary_keys:
            if is_datetime(existing_df[key].dtype):
                # The only way I could get this to work was by removing timezones
                # many attempts to coerc df to existing df with the correct tz
                df[key] = df[key].dt.tz_localize(None)
                df[key] = dd.to_datetime(df[key], utc=False)
                existing_df[key] = existing_df[key].dt.tz_localize(None)
                existing_df[key] = dd.to_datetime(existing_df[key], utc=False)
            elif df[key].dtype != existing_df[key].dtype:
                df[key] = df[key].astype(existing_df[key].dtype)


        outer_join = existing_df.merge(df, how = 'outer', on=primary_keys, indicator = True, suffixes=('_drop',''))
        new_rows = outer_join[(outer_join._merge == 'right_only')].drop('_merge', axis = 1)
        cols_to_drop = [x for x in new_rows.columns if x.endswith('_drop')]
        new_rows = new_rows.drop(columns=cols_to_drop)

        # Now concat with existing df
        new_rows = new_rows.astype(existing_df.dtypes)
        new_rows = new_rows[list(existing_df.columns)]
        final_df = dd.concat([existing_df, new_rows])

        if len(new_rows.index) > 0:
            final_df = final_df.repartition(npartitions=start_parts + existing_parts)

            # Coearce dtypes and make the columns the same order

            print("Copying cache for ``consistent'' upsert.")
            if fs.exists(temp_cache_path):
                fs.rm(temp_cache_path, recursive=True)

            write_parquet_helper(final_df, temp_cache_path, part)
            print("Successfully appended rows to temp parquet. Overwriting existing cache.")

            if fs.exists(cache_path):
                fs.rm(cache_path, recursive=True)

            fs.cp(temp_cache_path, cache_path, recursive=True)

        else:
            print("No rows to upsert.")
    else:
        if fs.exists(cache_path):
            fs.rm(cache_path, recursive=True)

        if isinstance(df, dd.DataFrame):
            write_parquet_helper(df, cache_path, part)
        elif isinstance(df, pd.DataFrame):
            df.to_parquet(cache_path, partition_cols=part, engine='pyarrow')
        else:
            raise ValueError("Can only write dask and pandas dataframes to parquet.")


@register_backend
class ParquetBackend(FileBackend):

    backend_name = 'parquet'

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, 'parquet')

    def write(self, data, upsert=False, primary_keys=None):
        if isinstance(data, dd.DataFrame):
            write_to_parquet(data, self.path, self.temp_cache_path, upsert=upsert, primary_keys=primary_keys)
        elif isinstance(data pd.DataFrame):
            if upsert:
                raise RuntimeError("Parquet backend does not support upsert for pandas engine.")

            data.to_parquet(self.path)
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")

    def read(self, engine):
        if engine == 'pandas' or engine == pd.DataFrame or engine==None:
            return return pd.read_parquet(self.path)
        elif engine == 'dask' or engine == dd.DataFrame:
            return dd.read_parquet(self.path, engine='pyarrow', ignore_metadata_file=True)
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")
