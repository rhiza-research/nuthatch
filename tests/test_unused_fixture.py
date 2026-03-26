import pytest

pytestmark = [pytest.mark.s3]


@pytest.mark.xfail(reason="Verifies fixture detects when cloud_storage is requested but never accessed", run=False)
def test_unused_cloud_storage(cloud_storage):
    """This test has markers but doesn't actually use cloud storage.

    This test verifies the validation in cloud_storage fixture works.
    The fixture should fail at teardown because the test never accesses
    cloud_storage["config"] or invokes any nuthatch operations.

    Note: We use run=False because xfail doesn't catch teardown errors.
    The actual verification happens by running:
        pytest tests/test_unused_fixture.py --runxfail
    which should show an ERROR at teardown.
    """
    assert True
