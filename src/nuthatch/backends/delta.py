from nuthatch.backend import FileBackend, register_backend
from deltalake import DeltaTable, write_deltalake
import dask.dataframe as dd
import pandas as pd
import dask_deltatable as ddt

@register_backend
class DeltaBackend(FileBackend):
    """
    Delta backend for caching tabular data in a delta table.

    This backend supports dask and pandas dataframes.
    """

    backend_name = "delta"
    default_for_type = pd.DataFrame

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, 'delta')


    def write(self, data, upsert=False, primary_keys=None):
        """Write a pandas dataframe to a delta table."""
        if upsert:
            raise ValueError("Delta backend does not support upsert.")

        if isinstance(data, dd.DataFrame):
            print("""Warning: Dask datafame passed to delta backend. Will run `compute()`
                      on the dataframe prior to storage. This will fail if the dataframe
                      does not fit in memory. Use `backend=parquet` to handle parallel writing of dask dataframes.""")
            write_data = data.compute()
        elif isinstance(data, pd.DataFrame):
            write_data = data
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")

        write_deltalake(self.path, write_data, mode='overwrite', schema_mode='overwrite')


    def read(self, engine=None):
        if engine == 'pandas' or engine == pd.DataFrame or engine is None:
            return DeltaTable(self.path).to_pandas()
        elif engine == 'dask' or engine == dd.DataFrame:
            return ddt.read_deltalake(self.path)
        else:
            raise RuntimeError("Delta backend only supports dask and pandas engines.")
