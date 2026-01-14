import pytest
from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask


pytestmark = [pytest.mark.s3, pytest.mark.gcs, pytest.mark.azure]


def test_parquet(cloud_storage):
    multi_tab_test_pandas(backend='parquet')
    multi_tab_test_dask(backend='parquet')
