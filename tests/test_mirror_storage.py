# Test storage backends differing from read backends (mirror functionality)
import copy
import pytest
import uuid
from nuthatch import cache


pytestmark = [pytest.mark.s3, pytest.mark.gcs, pytest.mark.azure]


def test_mirror(cloud_storage):
    """Test that mirror storage is used as a fallback for cached values.

    This test:
    1. First saves a value (5) to what will become the mirror location
    2. Then configures the mirror/root split
    3. Verifies the cached mirror value (5) is returned instead of the new value (10)
    """
    base_config = cloud_storage["config"]
    base_filesystem = base_config["root"]["filesystem"]
    test_id = uuid.uuid4()

    # Make a function that will be cached
    @cache(cache_args=[])
    def test_key(n):
        return n

    # Phase 1: Save value to what will become the mirror location
    config_phase1 = copy.deepcopy(base_config)
    config_phase1["root"]["filesystem"] = f"{base_filesystem}/mirror-{test_id}"

    with cloud_storage.config_context(config_phase1):
        test_key(5)

    # Phase 2: Configure mirror and root as separate locations
    config_phase2 = copy.deepcopy(base_config)
    config_phase2["root"]["filesystem"] = f"{base_filesystem}/root-{test_id}"
    config_phase2["mirror"] = copy.deepcopy(config_phase1["root"])

    with cloud_storage.config_context(config_phase2):
        # Should get the mirror value (5) not the computed value (10)
        result = test_key(10)
        assert result == 5
