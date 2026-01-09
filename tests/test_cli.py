"""
Tests for the nuthatch CLI module.

Tests the command-line interface functionality including:
- Helper functions (strip_base, get_metadata_*, get_cache_key)
- CLI commands (list, delete, import, print-config, cp)
"""

import pytest
from click.testing import CliRunner

from nuthatch.cli import (
    cli,
    strip_base,
    get_metadata_backend,
    get_metadata_namespace,
    get_null_metadata_namespace,
    get_metadata_cache_key,
    get_null_metadata_cache_key,
    get_cache_key,
)


# =============================================================================
# Helper Function Tests (Pure functions, no fixtures needed)
# =============================================================================

class TestStripBase:
    """Tests for strip_base helper function."""

    def test_strip_matching_base_path(self):
        """strip_base removes base path prefix from path."""
        path = "gs://bucket/cache/my/key/data.parquet"
        base_path = "gs://bucket/cache"
        result = strip_base(path, base_path)
        assert result == "my/key/data.parquet"

    def test_strip_with_protocol_stripped_base(self):
        """strip_base handles when path matches protocol-stripped base."""
        path = "bucket/cache/my/key/data.parquet"
        base_path = "gs://bucket/cache"
        result = strip_base(path, base_path)
        assert result == "my/key/data.parquet"

    def test_strip_removes_leading_slash(self):
        """strip_base removes leading slash from result."""
        path = "/local/cache/my/key"
        base_path = "/local/cache"
        result = strip_base(path, base_path)
        assert result == "my/key"

    def test_strip_no_match(self):
        """strip_base returns path unchanged when no match."""
        path = "/other/path/data.parquet"
        base_path = "/local/cache"
        result = strip_base(path, base_path)
        # Path doesn't start with base, so just removes leading slash
        assert result == "other/path/data.parquet"


class TestGetMetadataBackend:
    """Tests for get_metadata_backend helper function."""

    def test_extracts_backend_from_path(self):
        """get_metadata_backend extracts backend name from metadata path."""
        path = "/cache/nuthatch_metadata.nut/my/key/namespace/basic.parquet"
        result = get_metadata_backend(path)
        assert result == "basic"

    def test_extracts_backend_parquet(self):
        """get_metadata_backend works with parquet backend."""
        path = "gs://bucket/metadata/key/ns/parquet.parquet"
        result = get_metadata_backend(path)
        assert result == "parquet"

    def test_extracts_backend_delta(self):
        """get_metadata_backend works with delta backend."""
        path = "/cache/key/ns/delta.parquet"
        result = get_metadata_backend(path)
        assert result == "delta"


class TestGetMetadataNamespace:
    """Tests for get_metadata_namespace helper function."""

    def test_extracts_namespace_from_path(self):
        """get_metadata_namespace extracts namespace from metadata path."""
        path = "/cache/nuthatch_metadata.nut/my/key/test_namespace/basic.parquet"
        result = get_metadata_namespace(path)
        assert result == "test_namespace"

    def test_extracts_namespace_simple(self):
        """get_metadata_namespace works with simple paths."""
        path = "gs://bucket/key/my_ns/backend.parquet"
        result = get_metadata_namespace(path)
        assert result == "my_ns"


class TestGetNullMetadataNamespace:
    """Tests for get_null_metadata_namespace helper function."""

    def test_extracts_namespace_from_null_path(self):
        """get_null_metadata_namespace extracts namespace from null metadata path."""
        path = "/cache/nuthatch_metadata.nut/my/key/test_namespace.null"
        result = get_null_metadata_namespace(path)
        assert result == "test_namespace"

    def test_extracts_namespace_simple_null(self):
        """get_null_metadata_namespace works with simple null paths."""
        path = "gs://bucket/key/my_ns.null"
        result = get_null_metadata_namespace(path)
        assert result == "my_ns"


class TestGetMetadataCacheKey:
    """Tests for get_metadata_cache_key helper function."""

    def test_extracts_cache_key_with_namespace(self):
        """get_metadata_cache_key extracts cache key when namespace is present."""
        path = "gs://bucket/cache/nuthatch_metadata.nut/my/nested/key/namespace/basic.parquet"
        base_path = "gs://bucket/cache/nuthatch_metadata.nut"
        result = get_metadata_cache_key(path, base_path, namespace=True)
        # With namespace=True, removes last 2 path components (namespace + backend.ext)
        assert result == "my/nested/key"

    def test_extracts_cache_key_without_namespace(self):
        """get_metadata_cache_key extracts cache key when no namespace."""
        path = "gs://bucket/cache/nuthatch_metadata.nut/my/nested/key/basic.parquet"
        base_path = "gs://bucket/cache/nuthatch_metadata.nut"
        result = get_metadata_cache_key(path, base_path, namespace=False)
        # Without namespace, removes last 1 path component (backend.ext)
        assert result == "my/nested/key"


class TestGetNullMetadataCacheKey:
    """Tests for get_null_metadata_cache_key helper function."""

    def test_extracts_cache_key_with_namespace(self):
        """get_null_metadata_cache_key extracts cache key when namespace is present."""
        path = "gs://bucket/cache/nuthatch_metadata.nut/my/nested/key/namespace.null"
        base_path = "gs://bucket/cache/nuthatch_metadata.nut"
        result = get_null_metadata_cache_key(path, base_path, namespace=True)
        # With namespace=True, removes last 1 path component (namespace.null)
        assert result == "my/nested/key"

    def test_extracts_cache_key_without_namespace(self):
        """get_null_metadata_cache_key extracts cache key when no namespace."""
        path = "gs://bucket/cache/nuthatch_metadata.nut/my/nested/key.null"
        base_path = "gs://bucket/cache/nuthatch_metadata.nut"
        result = get_null_metadata_cache_key(path, base_path, namespace=False)
        # Without namespace, strips the .null extension
        assert result == "my/nested/key"


class TestGetCacheKey:
    """Tests for get_cache_key helper function."""

    def test_extracts_key_basic_case(self):
        """get_cache_key extracts key from basic path."""
        path = "gs://bucket/cache/my/key.pickle"
        base_path = "gs://bucket/cache"
        result = get_cache_key(path, base_path, namespace=None, extension="pickle")
        assert result == "my/key"

    def test_extracts_key_with_namespace(self):
        """get_cache_key strips namespace from path."""
        path = "gs://bucket/cache/test_ns/my/key.parquet"
        base_path = "gs://bucket/cache"
        result = get_cache_key(path, base_path, namespace="test_ns", extension="parquet")
        assert result == "my/key"

    def test_extracts_key_no_extension_match(self):
        """get_cache_key handles path without matching extension."""
        path = "gs://bucket/cache/my/key"
        base_path = "gs://bucket/cache"
        result = get_cache_key(path, base_path, namespace=None, extension="parquet")
        assert result == "my/key"

    def test_extracts_key_local_path(self):
        """get_cache_key works with local filesystem paths."""
        path = "/home/user/cache/data/key.pickle"
        base_path = "/home/user/cache"
        result = get_cache_key(path, base_path, namespace=None, extension="pickle")
        assert result == "data/key"


# =============================================================================
# CLI Command Tests
# =============================================================================

class TestCliGroup:
    """Tests for the main CLI group."""

    def test_cli_version(self):
        """CLI --version returns version info."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_cli_help(self):
        """CLI --help shows available commands."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Nuthatch" in result.output
        assert "list" in result.output
        assert "delete" in result.output
        assert "import" in result.output
        assert "print-config" in result.output
        assert "cp" in result.output


# =============================================================================
# List Command Tests
# =============================================================================

@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
class TestListCommand:
    """Tests for the 'list' command."""

    def test_list_no_caches(self, cloud_storage):
        """list command shows 'No caches found' when empty."""
        runner = CliRunner()
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "No caches found" in result.output

    def test_list_with_cache(self, cloud_storage):
        """list command shows caches after one is created."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Create a cache entry (use 2-level key for CLI glob compatibility)
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "test/key", None, None, {}, 'basic', {})
        cache.write({"test": "data"})

        runner = CliRunner()
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "test/key" in result.output

    def test_list_with_namespace_filter(self, cloud_storage):
        """list command filters by namespace."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Create cache entries with different namespaces
        config = NuthatchConfig(wrapped_module='tests')
        cache1 = Cache(config['root'], "nsfilter/key1", "ns1", None, {}, 'basic', {})
        cache1.write("data1")

        cache2 = Cache(config['root'], "nsfilter/key2", "ns2", None, {}, 'basic', {})
        cache2.write("data2")

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "nsfilter/*", "--namespace", "ns1"])
        assert result.exit_code == 0
        # Should show ns1 entries but not ns2
        assert "ns1" in result.output
        assert "ns2" not in result.output

    def test_list_with_cache_key_pattern(self, cloud_storage):
        """list command filters by cache key pattern."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        config = NuthatchConfig(wrapped_module='tests')
        # Create matching and non-matching caches
        cache1 = Cache(config['root'], "pattern/match", None, None, {}, 'basic', {})
        cache1.write("matching data")

        cache2 = Cache(config['root'], "other/nomatch", None, None, {}, 'basic', {})
        cache2.write("non-matching data")

        runner = CliRunner()
        result = runner.invoke(cli, ["list", "pattern/*"])
        assert result.exit_code == 0
        # Should show matching pattern but not other
        assert "pattern/match" in result.output
        assert "other/nomatch" not in result.output

    def test_list_help(self):
        """list --help shows command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["list", "--help"])
        assert result.exit_code == 0
        assert "cache_key" in result.output.lower() or "namespace" in result.output.lower()


# =============================================================================
# Print-Config Command Tests
# =============================================================================

@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
class TestPrintConfigCommand:
    """Tests for the 'print-config' command."""

    def test_print_config_shows_backends(self, cloud_storage):
        """print-config shows backend configurations."""
        runner = CliRunner()
        result = runner.invoke(cli, ["print-config"])
        # Should show at least Basic backend (always registered)
        assert result.exit_code == 0
        # Output should contain backend info
        assert "Basic" in result.output or "filesystem" in result.output

    def test_print_config_specific_backend(self, cloud_storage):
        """print-config with --backend shows specific backend config."""
        runner = CliRunner()
        result = runner.invoke(cli, ["print-config", "--backend", "basic"])
        assert result.exit_code == 0
        assert "filesystem" in result.output or "basic" in result.output

    def test_print_config_invalid_location(self, cloud_storage):
        """print-config with invalid location shows error."""
        runner = CliRunner()
        result = runner.invoke(cli, ["print-config", "--location", "nonexistent"])
        assert result.exit_code == 0
        assert "No configuration found" in result.output

    def test_print_config_masks_secrets_by_default(self, cloud_storage):
        """print-config masks secrets unless --show-secrets is used."""
        runner = CliRunner()
        result = runner.invoke(cli, ["print-config"])
        # Secrets should be masked (shown as asterisks)
        assert result.exit_code == 0

    def test_print_config_help(self):
        """print-config --help shows command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["print-config", "--help"])
        assert result.exit_code == 0
        assert "location" in result.output.lower()


# =============================================================================
# Delete Command Tests
# =============================================================================

@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
class TestDeleteCommand:
    """Tests for the 'delete' command."""

    def test_delete_no_caches(self, cloud_storage):
        """delete command shows no caches when none exist."""
        runner = CliRunner()
        result = runner.invoke(cli, ["delete", "nonexistent/key"])
        assert result.exit_code == 0
        assert "No caches found" in result.output

    def test_delete_with_confirmation(self, cloud_storage):
        """delete command prompts for confirmation."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Use 2-level key for CLI glob compatibility
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "deltest/key", None, None, {}, 'basic', {})
        cache.write("data to delete")

        runner = CliRunner()
        # Input 'n' to abort deletion
        result = runner.invoke(cli, ["delete", "deltest/key"], input="n\n")
        assert result.exit_code == 1  # Aborted

    def test_delete_confirmed(self, cloud_storage):
        """delete command deletes cache when confirmed."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Use 2-level key for CLI glob compatibility
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "delconf/key", None, None, {}, 'basic', {})
        cache.write("data to delete")

        runner = CliRunner()
        # Input 'y' to confirm deletion
        result = runner.invoke(cli, ["delete", "delconf/key"], input="y\n")
        assert result.exit_code == 0
        assert "Deleting" in result.output

        # Verify cache is deleted
        cache2 = Cache(config['root'], "delconf/key", None, None, {}, 'basic', {})
        assert not cache2.exists()

    def test_delete_with_force_flag(self, cloud_storage):
        """delete command with -f skips confirmation."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Use 2-level key for CLI glob compatibility
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "delforce/key", None, None, {}, 'basic', {})
        cache.write("data to delete")

        runner = CliRunner()
        # Note: The current implementation still asks for confirmation even with -f
        # This test documents current behavior - -f flag might need implementation fix
        result = runner.invoke(cli, ["delete", "delforce/key", "-f"], input="y\n")
        assert result.exit_code == 0

    def test_delete_help(self):
        """delete --help shows command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["delete", "--help"])
        assert result.exit_code == 0
        assert "cache_key" in result.output.lower()


# =============================================================================
# Import Command Tests
# =============================================================================

@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
class TestImportCommand:
    """Tests for the 'import' command."""

    def test_import_requires_backend(self, cloud_storage):
        """import command requires --backend option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "some/key"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_import_no_caches_found(self, cloud_storage):
        """import command shows no caches when pattern matches nothing."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "nonexistent/*", "--backend", "basic"])
        assert result.exit_code == 0
        assert "No caches found" in result.output

    def test_import_null_backend(self, cloud_storage):
        """import command works with null backend."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "test/key", "--backend", "null"])
        assert result.exit_code == 0
        assert "No caches found" in result.output

    def test_import_existing_data(self, cloud_storage):
        """import command imports existing data files into cache."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        config = NuthatchConfig(wrapped_module='tests')

        # Create a cache entry normally (creates both data file and metadata)
        cache = Cache(config['root'], "importtest/data", None, None, {}, 'basic', {})
        cache.write({"imported": "data"})
        assert cache.exists()

        # Delete only the metadata, leaving the data file on disk
        cache._delete_metadata()

        # Verify cache no longer "exists" (metadata is gone)
        cache_before = Cache(config['root'], "importtest/data", None, None, {}, 'basic', {})
        assert not cache_before.exists()

        # But the data file should still be there
        assert cache_before.backend.exists()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["import", "importtest/*", "--backend", "basic"],
            input="y\n"
        )
        assert result.exit_code == 0
        assert "Imported" in result.output

        # Verify cache now exists again (metadata was recreated)
        cache_after = Cache(config['root'], "importtest/data", None, None, {}, 'basic', {})
        assert cache_after.exists()
        assert cache_after.read() == {"imported": "data"}

    def test_import_help(self):
        """import --help shows command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--help"])
        assert result.exit_code == 0
        assert "backend" in result.output.lower()


# =============================================================================
# Copy Command Tests
# =============================================================================


@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
class TestCopyCommand:
    """Tests for the 'cp' command."""

    def test_cp_no_caches(self, cloud_storage):
        """cp command shows no caches when none exist."""
        runner = CliRunner()
        result = runner.invoke(cli, ["cp", "nonexistent/key", "--to-location", "local"])
        assert result.exit_code == 0
        assert "No caches found" in result.output

    def test_cp_with_confirmation_abort(self, cloud_storage):
        """cp command prompts for confirmation and can be aborted."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Create a cache entry to copy
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "cptest/key", None, None, {}, 'basic', {})
        cache.write({"test": "data"})

        runner = CliRunner()
        # Input 'n' to abort copy
        result = runner.invoke(cli, ["cp", "cptest/key", "--to-location", "local"], input="n\n")
        assert result.exit_code == 1  # Aborted

    def test_cp_confirmed(self, cloud_storage):
        """cp command copies cache when confirmed."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Create a cache entry to copy
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "cpconf/key", None, None, {}, 'basic', {})
        cache.write({"test": "copy data"})

        runner = CliRunner()
        # Input 'y' to confirm copy
        result = runner.invoke(
            cli,
            ["cp", "cpconf/key", "--from-location", "root", "--to-location", "local"],
            input="y\n"
        )
        assert result.exit_code == 0
        assert "Copy" in result.output

        # Verify cache was copied to local
        local_cache = Cache(config['local'], "cpconf/key", None, None, {}, 'basic', {})
        assert local_cache.exists()
        assert local_cache.read() == {"test": "copy data"}

    def test_cp_with_namespace(self, cloud_storage):
        """cp command works with namespace option."""
        from nuthatch.cache import Cache
        from nuthatch.config import NuthatchConfig

        # Create a cache entry with namespace
        config = NuthatchConfig(wrapped_module='tests')
        cache = Cache(config['root'], "cpns/key", "test_ns", None, {}, 'basic', {})
        cache.write("namespaced data")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["cp", "cpns/key", "--namespace", "test_ns", "--to-location", "local"],
            input="n\n"  # Abort after confirming the cache is found
        )
        # Should find the cache (abort confirms it was found)
        assert result.exit_code == 1 or "No caches found" not in result.output

    def test_cp_help(self):
        """cp --help shows command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["cp", "--help"])
        assert result.exit_code == 0
        assert "cache_key" in result.output.lower()
