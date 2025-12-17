import pytest

pytestmark = [pytest.mark.s3]


@pytest.mark.xfail(reason="Verifies fixture detects when cloud_storage is requested but never accessed")
def test_unused_cloud_storage(cloud_storage):
    """This test has markers but doesn't actually use cloud storage."""
    assert True
