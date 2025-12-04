from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask

def test_parquet():
    multi_tab_test_pandas(backend='parquet')
    multi_tab_test_dask(backend='parquet')
