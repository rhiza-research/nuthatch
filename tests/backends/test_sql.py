from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask, pandas_df
import pandas as pd


def test_sql(sql_storage):
    multi_tab_test_pandas(backend='sql')
    multi_tab_test_dask(backend='sql')

# Test sql as storage backend
def test_sql_storage_backend(sql_storage):
    df = pandas_df(name="oak", backend='delta', storage_backend='sql', cache_mode='overwrite')
    df2 = pandas_df(name="oak", backend='sql')
    pd.testing.assert_frame_equal(df, df2)
