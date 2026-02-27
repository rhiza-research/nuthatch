import pytest
from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask


pytestmark = [pytest.mark.cloud]


def test_delta(cloud_storage):
    multi_tab_test_pandas(backend='delta')
    multi_tab_test_dask(backend='delta')
