# Test storage backends differing from read backends (mirror functionality)
import pytest
import uuid
from nuthatch import cache
from nuthatch.config import set_test_config_provider


pytestmark = [pytest.mark.s3, pytest.mark.gcs, pytest.mark.azure]


def test_mirror(cloud_storage, tmp_path):
    """Test that mirror storage is used as a fallback for cached values.

    This test:
    1. First saves a value (5) to what will become the mirror location
    2. Then configures the mirror/root split
    3. Verifies the cached mirror value (5) is returned instead of the new value (10)
    """
    # Get base config from the cloud_storage fixture
    base_config = cloud_storage["config"]
    base_filesystem = base_config["root"]["filesystem"]
    base_options = base_config["root"].get("filesystem_options", {})

    # Create unique subpaths for mirror and root
    test_id = uuid.uuid4()
    mirror_filesystem = f"{base_filesystem}/mirror-{test_id}"
    root_filesystem = f"{base_filesystem}/root-{test_id}"

    # Make a function that will be cached
    @cache(cache_args=[])
    def test_key(n):
        return n

    # Phase 1: Save value to what will become the mirror location
    # Configure with mirror path as root
    config_phase1 = {
        'root': {
            'filesystem': mirror_filesystem,
            'filesystem_options': base_options,
            'metadata_location': 'filesystem',
        },
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }
    set_test_config_provider(lambda: config_phase1)

    # Save 5 in what will become the mirror
    d = test_key(5)

    # Phase 2: Now configure with mirror and root as separate locations
    config_phase2 = {
        'root': {
            'filesystem': root_filesystem,
            'filesystem_options': base_options,
            'metadata_location': 'filesystem',
        },
        'mirror': {
            'filesystem': mirror_filesystem,
            'filesystem_options': base_options,
            'metadata_location': 'filesystem',
        },
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }
    set_test_config_provider(lambda: config_phase2)

    # Should get the mirror value (5) not the computed value (10)
    d = test_key(10)
    assert d == 5
