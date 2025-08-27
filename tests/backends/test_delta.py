from tabular_test import multi_tab_test_pandas, multi_tab_test_dask
from nuthatch.config import config_parameter
from google.cloud import secretmanager

def test_delta():
    multi_tab_test_pandas(backend='delta')
    multi_tab_test_dask(backend='delta')
