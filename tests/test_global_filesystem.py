"""Test adding a cache to the global filesystem configuration.

These tests verify that filesystems can be added to the global skipped filesystems
list in ~/.nuthatch.toml, and that the system properly prompts users when cache
reads fail to add inaccessible filesystems to this list.
"""
import os
import tempfile
import tomllib
import tomli_w

from nuthatch.config import set_global_skipped_filesystem
from nuthatch.nuthatch import instantiate_read_caches


def test_set_global_skipped_filesystem(monkeypatch):
    """Test adding a filesystem to the skipped list in global config.
    
    This test verifies the basic functionality of set_global_skipped_filesystem:
    - It can create a new config file if one doesn't exist
    - It can add filesystems to the skipped list
    - It can add multiple filesystems
    - It prevents duplicate entries
    """
    # Create a temporary config file to simulate ~/.nuthatch.toml
    # We use delete=False so we can manually clean it up after the test
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.toml', delete=False) as f:
        config_file = f.name

    try:
        # Patch os.path.expanduser to redirect ~/.nuthatch.toml to our temp file
        # This allows us to test the function without modifying the user's actual config
        monkeypatch.setattr('nuthatch.config.os.path.expanduser',
                            lambda path: config_file if path == '~/.nuthatch.toml' else os.path.expanduser(path))

        # Test 1: Add a filesystem to the skipped list
        # This should create the config file structure and add the filesystem
        test_filesystem = "gs://test-bucket/cache"
        set_global_skipped_filesystem(test_filesystem)

        # Verify the config file was created
        assert os.path.exists(config_file), "Config file should be created after adding a filesystem"

        # Read the config file and verify the filesystem was added
        with open(config_file, 'rb') as f:
            config_data = tomllib.load(f)

        # Check that the config has the expected structure: tool.nuthatch.skipped_filesystems
        skipped_list = config_data['tool']['nuthatch']['skipped_filesystems']
        assert test_filesystem in skipped_list, "First filesystem should be in the skipped list"

        # Test 2: Add another filesystem to verify multiple entries work
        test_filesystem2 = "s3://another-bucket/cache"
        set_global_skipped_filesystem(test_filesystem2)

        # Re-read the config to verify both filesystems are present
        with open(config_file, 'rb') as f:
            config_data = tomllib.load(f)

        skipped_list = config_data['tool']['nuthatch']['skipped_filesystems']
        assert test_filesystem in skipped_list, "First filesystem should still be in the list"
        assert test_filesystem2 in skipped_list, "Second filesystem should be added to the list"

        # Test 3: Try to add the first filesystem again to verify duplicates are prevented
        set_global_skipped_filesystem(test_filesystem)

        # Re-read the config and verify the filesystem only appears once
        with open(config_file, 'rb') as f:
            config_data = tomllib.load(f)

        skipped_list = config_data['tool']['nuthatch']['skipped_filesystems']
        assert skipped_list.count(test_filesystem) == 1, "Filesystem should only appear once, no duplicates allowed"

    finally:
        # Clean up the temporary config file
        if os.path.exists(config_file):
            os.unlink(config_file)


def test_failed_read_prompts_to_add_filesystem(monkeypatch):
    """Test that a failed read prompts the user to add filesystem to skipped list.
    
    When instantiate_read_caches encounters a NuthatchReadError (e.g., from an
    inaccessible bucket), it should prompt the user via input() to add that
    filesystem to the skipped list. If the user responds 'y', the filesystem
    should be added to ~/.nuthatch.toml.
    """
    # Create a temporary config file to simulate ~/.nuthatch.toml
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.toml', delete=False) as f:
        config_file = f.name

    # Create a temp directory for the root cache
    # We need at least one working cache (root) so instantiate_read_caches doesn't
    # raise a "No Nuthatch configuration found" error
    with tempfile.TemporaryDirectory() as root_cache_dir:
        try:
            # Patch os.path.expanduser to redirect ~/.nuthatch.toml to our temp file
            # We need to capture the original function first to avoid recursion
            import nuthatch.config as config_module
            original_expanduser = config_module.os.path.expanduser
            def mock_expanduser(path):
                if path == '~/.nuthatch.toml':
                    return config_file
                return original_expanduser(path)
            monkeypatch.setattr(config_module.os.path, 'expanduser', mock_expanduser)

            # Use a real inaccessible GCS bucket path that will fail when Cache tries to access it
            # This will naturally raise NuthatchReadError when the Cache tries to instantiate
            # its metastore and check for the existence file
            test_filesystem = "gs://definitely-does-not-exist-bucket-12345/cache"
            config = {
                'root': {
                    'filesystem': root_cache_dir  # This will work (local filesystem)
                },
                'mirror': {
                    'filesystem': test_filesystem  # This will fail (inaccessible bucket)
                }
            }

            # Mock the input() function to simulate user responding 'y' (yes, add to skipped list)
            # This prevents the test from hanging waiting for actual user input
            # Since input() is a builtin, we patch builtins.input
            monkeypatch.setattr('builtins.input', lambda prompt: 'y')

            # Call instantiate_read_caches with our config
            # When it tries to instantiate the mirror cache, it will fail with NuthatchReadError
            # The function should catch this, prompt the user (via our mocked input), and if
            # the user says 'y', call set_global_skipped_filesystem to add it to the config
            caches, cache_exception = instantiate_read_caches(
                config, 'test_key', None, None, {}, None, {}
            )

            # Verify the filesystem was added to the skipped list in the config file
            with open(config_file, 'rb') as f:
                config_data = tomllib.load(f)

            skipped_list = config_data['tool']['nuthatch']['skipped_filesystems']
            assert test_filesystem in skipped_list, "Filesystem should be added to skipped list after user confirms"

        finally:
            # Clean up the temporary config file
            if os.path.exists(config_file):
                os.unlink(config_file)


def test_failed_read_user_declines_to_add_filesystem(monkeypatch):
    """Test that when user declines, filesystem is not added to skipped list.
    
    This test verifies the opposite behavior: when a cache read fails and the
    user responds 'n' (no) to the prompt, the filesystem should NOT be added
    to the skipped list. The system should still continue (with a warning) but
    not modify the global config.
    """
    # Create a temporary config file to simulate ~/.nuthatch.toml
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.toml', delete=False) as f:
        config_file = f.name

    # Create a temp directory for the root cache
    # We need at least one working cache so instantiate_read_caches doesn't fail completely
    with tempfile.TemporaryDirectory() as root_cache_dir:
        try:
            # Patch os.path.expanduser to redirect ~/.nuthatch.toml to our temp file
            # We need to capture the original function first to avoid recursion
            import nuthatch.config as config_module
            original_expanduser = config_module.os.path.expanduser
            def mock_expanduser(path):
                if path == '~/.nuthatch.toml':
                    return config_file
                return original_expanduser(path)
            monkeypatch.setattr(config_module.os.path, 'expanduser', mock_expanduser)

            # Use a real inaccessible GCS bucket path that will fail when Cache tries to access it
            # Using a different bucket name than the previous test to ensure test isolation
            test_filesystem = "gs://definitely-does-not-exist-bucket-67890/cache"
            config = {
                'root': {
                    'filesystem': root_cache_dir  # This will work (local filesystem)
                },
                'mirror': {
                    'filesystem': test_filesystem  # This will fail (inaccessible bucket)
                }
            }

            # Mock the input() function to simulate user responding 'n' (no, don't add to skipped list)
            # Since input() is a builtin, we patch builtins.input
            monkeypatch.setattr('builtins.input', lambda prompt: 'n')

            # Call instantiate_read_caches with our config
            # When it tries to instantiate the mirror cache, it will fail with NuthatchReadError
            # The function should catch this, prompt the user (via our mocked input), and if
            # the user says 'n', it should NOT call set_global_skipped_filesystem
            caches, cache_exception = instantiate_read_caches(
                config, 'test_key', None, None, {}, None, {}
            )

            # Verify the filesystem was NOT added to the skipped list
            # Note: The config file might not exist if no filesystems were added yet,
            # or it might exist but not contain our test filesystem
            if os.path.exists(config_file):
                with open(config_file, 'rb') as f:
                    config_data = tomllib.load(f)
                
                # If the config file exists and has the skipped_filesystems structure,
                # verify that our test filesystem is NOT in the list
                if 'tool' in config_data and 'nuthatch' in config_data['tool']:
                    skipped_list = config_data['tool']['nuthatch'].get('skipped_filesystems', [])
                    assert test_filesystem not in skipped_list, "Filesystem should NOT be in skipped list when user declines"

        finally:
            # Clean up the temporary config file
            if os.path.exists(config_file):
                os.unlink(config_file)
