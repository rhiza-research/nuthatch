"""
Pytest fixtures for cloud storage testing.

This module provides fixtures for testing nuthatch backends against
cloud storage providers (S3, GCS, Azure Blob Storage) and PostgreSQL.

Cloud storage configuration is read from terraform-generated config files:
- nuthatch.aws.toml: S3 credentials (from test_infrastructure/aws.tf)
- nuthatch.gcp.toml: GCS credentials (from test_infrastructure/gcp.tf)
- nuthatch.azure.toml: Azure credentials (from test_infrastructure/azure.tf)

Usage:
    # Run tests against real cloud test infrastructure:
    pytest -m gcs
    pytest -m s3
    pytest -m azure

    # Run in Docker with emulators (uses .docker.toml config files):
    docker compose -f docker-compose.test.yml run --rm test
"""

import os
import uuid
import pytest

import nuthatch.config


def skip_azure_delta_on_emulator(cloud_storage):
    """Skip test if using delta backend on Azure emulator (Azurite doesn't support HNS)."""
    if os.environ.get('NUTHATCH_EMULATOR') and cloud_storage['provider'] == 'azure':
        pytest.skip("Delta Lake requires ADLS Gen2 (Azurite doesn't support HNS)")



def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "s3: marks tests as requiring S3/LocalStack")
    config.addinivalue_line("markers", "gcs: marks tests as requiring GCS/fake-gcs-server")
    config.addinivalue_line("markers", "azure: marks tests as requiring Azure/Azurite")


def pytest_sessionfinish(session, exitstatus):
    """Clear fsspec caches and detach finalizers to prevent async cleanup errors at exit."""
    import gc
    import weakref

    # Force close all aiohttp connectors synchronously
    try:
        import aiohttp
        for obj in gc.get_objects():
            try:
                if isinstance(obj, aiohttp.TCPConnector) and not obj.closed:
                    obj._close()
            except Exception:
                pass
    except ImportError:
        pass

    # Clear fsspec caches
    try:
        import gcsfs
        gcsfs.GCSFileSystem.clear_instance_cache()
    except ImportError:
        pass

    try:
        import s3fs
        s3fs.S3FileSystem.clear_instance_cache()
    except ImportError:
        pass

    try:
        import adlfs
        adlfs.AzureBlobFileSystem.clear_instance_cache()
    except ImportError:
        pass

    # Detach fsspec finalizers that would fail at interpreter exit
    for ref in list(weakref.finalize._registry):
        try:
            info = weakref.finalize._registry.get(ref)
            if info and getattr(info.func, '__name__', '') in ('close_session', 'sync'):
                ref.detach()
        except Exception:
            pass

    gc.collect()

    # Detach any new finalizers created during GC
    for ref in list(weakref.finalize._registry):
        try:
            info = weakref.finalize._registry.get(ref)
            if info and getattr(info.func, '__name__', '') in ('close_session', 'sync'):
                ref.detach()
        except Exception:
            pass


def pytest_generate_tests(metafunc):
    """Parametrize cloud_storage based on which provider markers are on the test.

    Respects -m filter: if -m gcs is passed, only parametrize with gcs even if
    the test also has s3/azure markers.
    """
    if "cloud_storage" not in metafunc.fixturenames:
        return

    # Get the marker expression from CLI (e.g., "gcs", "s3", "gcs or s3")
    markexpr = metafunc.config.getoption("-m", default="")

    # Check which provider markers are on this test
    all_markers = []
    filtered_providers = []
    for provider in ["gcs", "s3", "azure"]:
        if metafunc.definition.get_closest_marker(provider):
            all_markers.append(provider)
            # If a marker filter is set, only include providers that match
            if markexpr and provider not in markexpr:
                continue
            filtered_providers.append(provider)

    # Fail if test has NO markers at all (developer error)
    if not all_markers:
        pytest.fail(f"Test {metafunc.function.__name__} requests cloud_storage but has no provider markers (@pytest.mark.s3, @pytest.mark.gcs, @pytest.mark.azure)")

    # If markers exist but none match the filter, skip parametrization (test will be deselected)
    if not filtered_providers:
        return

    metafunc.parametrize("cloud_storage", filtered_providers, indirect=True)


# =============================================================================
# Cloud Storage Fixture
#
# Tests request cloud_storage and mark which providers they support:
#   @pytest.mark.s3
#   @pytest.mark.gcs
#   @pytest.mark.azure
#   def test_something(cloud_storage):
#       ...
# =============================================================================


@pytest.fixture
def cloud_storage(request, tmp_path, monkeypatch):
    """
    Configure nuthatch for the parametrized cloud provider.

    Reads credentials from terraform-generated config files (nuthatch.aws.toml, etc.)
    and modifies the filesystem path to use a unique test prefix for isolation.
    """
    import tomllib
    import tomli_w
    import copy
    from pathlib import Path
    from contextlib import contextmanager

    # Clear any module-level @config_parameter registrations to prevent test pollution
    nuthatch.config.dynamic_parameters.clear()

    provider = request.param

    # Map provider to config file
    config_files = {
        "s3": "nuthatch.aws.toml",
        "gcs": "nuthatch.gcp.toml",
        "azure": "nuthatch.azure.toml",
    }
    project_root = Path(__file__).parent.parent
    config_file = project_root / config_files[provider]

    # Read config and make a copy for modification
    with open(config_file, "rb") as f:
        config = copy.deepcopy(tomllib.load(f))

    # Resolve relative paths to absolute (needed when config is copied to temp dir)
    fs_options = config.get("root", {}).get("filesystem_options", {})
    if "token" in fs_options and fs_options["token"] != "anon":
        token_path = Path(fs_options["token"])
        if not token_path.is_absolute():
            fs_options["token"] = str(project_root / token_path)

    # Override filesystem path with unique test prefix for isolation
    # Preserve protocol and bucket/container from config, just add unique path suffix
    test_cache_id = uuid.uuid4().hex[:8]
    original_filesystem = config["root"]["filesystem"]
    # Parse: "protocol://bucket/path" -> keep protocol and bucket, replace path
    protocol_and_bucket = "/".join(original_filesystem.split("/")[0:3])
    config["root"]["filesystem"] = f"{protocol_and_bucket}/cache-{test_cache_id}"

    # For emulators, buckets/containers are pre-created by docker-compose init services
    # For real cloud, buckets are pre-created by terraform

    # Override local cache path
    config.setdefault("local", {})["filesystem"] = str(tmp_path / 'local_cache')

    # Write modified config to temp file for tests that read config from disk
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    temp_config_file = tmp_path / "nuthatch.toml"
    with open(temp_config_file, "wb") as f:
        tomli_w.dump(config, f)

    # Set env vars to point to temp config (for tests that read config from disk)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv(nuthatch.config.NUTHATCH_PROJECT_CONFIG_ENV, str(temp_config_file))
    monkeypatch.setenv(nuthatch.config.NUTHATCH_GLOBAL_CONFIG_ENV, str(fake_home / ".nuthatch.toml"))

    class CloudStorageResult(dict):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._accessed = False

        def __getitem__(self, key):
            self._accessed = True
            return super().__getitem__(key)

    result = CloudStorageResult({
        "provider": provider,
        "config": config,
    })

    @contextmanager
    def config_context(custom_config):
        """Temporarily swap the nuthatch config on disk."""
        result._accessed = True
        with open(temp_config_file, "wb") as f:
            tomli_w.dump(custom_config, f)
        try:
            yield
        finally:
            with open(temp_config_file, "wb") as f:
                tomli_w.dump(config, f)

    result.config_context = config_context

    # Track NuthatchConfig construction as evidence of cloud storage usage
    original_init = nuthatch.config.NuthatchConfig.__init__

    def tracking_init(self_inner, *args, **kwargs):
        result._accessed = True
        return original_init(self_inner, *args, **kwargs)

    monkeypatch.setattr(nuthatch.config.NuthatchConfig, '__init__', tracking_init)

    yield result

    if not result._accessed:
        pytest.fail(
            f"Test {request.node.name} requests cloud_storage but never used it. "
            "Remove cloud provider markers and use a local config fixture instead."
        )


# =============================================================================
# Local-Only Fixture
#
# For tests that need nuthatch config but don't use cloud storage.
# =============================================================================


@pytest.fixture
def local_config(tmp_path, monkeypatch):
    """Configure nuthatch with local filesystem paths only.

    For tests that need valid nuthatch config but don't interact with
    cloud storage (e.g., memoizer tests, local caching tests).
    """
    import tomli_w

    nuthatch.config.dynamic_parameters.clear()

    root_cache = tmp_path / "root_cache"
    local_cache = tmp_path / "local_cache"
    root_cache.mkdir()
    local_cache.mkdir()

    config = {
        "root": {"filesystem": str(root_cache)},
        "local": {"filesystem": str(local_cache)},
    }

    fake_home = tmp_path / "home"
    fake_home.mkdir()
    temp_config_file = tmp_path / "nuthatch.toml"
    with open(temp_config_file, "wb") as f:
        tomli_w.dump(config, f)

    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv(nuthatch.config.NUTHATCH_PROJECT_CONFIG_ENV, str(temp_config_file))
    monkeypatch.setenv(nuthatch.config.NUTHATCH_GLOBAL_CONFIG_ENV, str(fake_home / ".nuthatch.toml"))

    yield

    nuthatch.config.dynamic_parameters.clear()


# =============================================================================
# =============================================================================
# PostgreSQL Fixtures (for SQL backend testing)
# =============================================================================

@pytest.fixture(scope="session")
def postgres_credentials():
    """Get PostgreSQL credentials from environment.

    Environment variables:
    - POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
    """
    host = os.environ.get("POSTGRES_HOST")
    if not host:
        pytest.skip("PostgreSQL not configured (set POSTGRES_HOST)")

    return {
        "driver": "postgresql",
        "username": os.environ.get("POSTGRES_USER", "test"),
        "password": os.environ.get("POSTGRES_PASSWORD", "test"),
        "host": host,
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "nuthatch_test"),
    }


@pytest.fixture
def postgres_storage(request, postgres_credentials, tmp_path, monkeypatch):
    """Configure nuthatch to use PostgreSQL for SQL backend."""
    import tomli_w
    import sqlalchemy

    # Clear any module-level @config_parameter registrations to prevent test pollution
    nuthatch.config.dynamic_parameters.clear()

    # Check if this test uses terracotta - if so, use a unique database name
    # because terracotta's create() tries to CREATE DATABASE (we don't create it here)
    test_name = request.node.name
    if 'terracotta' in test_name:
        # Use a unique database name - terracotta will create it
        db_name = f"tc_{uuid.uuid4().hex[:8]}"
        credentials = {**postgres_credentials, 'database': db_name}
    else:
        credentials = postgres_credentials

    config = {
        'root': {
            'filesystem': str(tmp_path / 'nuthatch_cache'),
            **credentials,
        }
    }

    # Write config to temp file and point nuthatch at it via env vars
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    temp_config_file = tmp_path / "nuthatch.toml"
    with open(temp_config_file, "wb") as f:
        tomli_w.dump(config, f)

    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv(nuthatch.config.NUTHATCH_PROJECT_CONFIG_ENV, str(temp_config_file))
    monkeypatch.setenv(nuthatch.config.NUTHATCH_GLOBAL_CONFIG_ENV, str(fake_home / ".nuthatch.toml"))

    yield {"credentials": credentials, "config": config}

    # Cleanup: drop the terracotta database if we created one
    if 'terracotta' in test_name:
        admin_url = f"postgresql://{postgres_credentials['username']}:{postgres_credentials['password']}@{postgres_credentials['host']}:{postgres_credentials['port']}/postgres"
        engine = sqlalchemy.create_engine(admin_url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            # Terminate connections to the database before dropping
            conn.execute(sqlalchemy.text(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
            """))
            conn.execute(sqlalchemy.text(f"DROP DATABASE IF EXISTS {db_name}"))
        engine.dispose()
