from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask

def test_delta():
    multi_tab_test_pandas(backend='delta')
    multi_tab_test_dask(backend='delta')
