import os
import pytest
from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask


pytestmark = [pytest.mark.cloud]


def test_delta(cloud_storage):
    # Azurite doesn't support HNS (Hierarchical Namespace) required by Delta Lake
    if os.environ.get('NUTHATCH_EMULATOR') and cloud_storage['provider'] == 'azure':
        pytest.skip("Delta Lake requires ADLS Gen2 (Azurite doesn't support HNS)")
    multi_tab_test_pandas(backend='delta')
    multi_tab_test_dask(backend='delta')
