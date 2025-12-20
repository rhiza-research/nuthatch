from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask


def test_sql(postgres_storage):
    multi_tab_test_pandas(backend='sql')
    multi_tab_test_dask(backend='sql')
