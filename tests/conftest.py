"""
Pytest fixtures for cloud storage testing.

This module provides fixtures for testing nuthatch backends against
cloud storage providers (S3, GCS, Azure Blob Storage) and PostgreSQL.

Configuration is entirely environment-driven:
- Docker (docker-compose.test.yml): Environment variables point to emulators
  (LocalStack, fake-gcs-server, Azurite) running in isolated network
- Local (.envrc): Environment variables contain real cloud credentials

Usage:
    # Run in Docker with emulators (isolated, no internet):
    docker compose -f docker-compose.test.yml run --rm test

    # Run locally with real cloud credentials:
    pytest -m gcs
"""

import os
import uuid
import pytest

import nuthatch.config


class test_config:
    """Context manager for setting test config.

    Usage:
        with test_config({'root': {'filesystem': 's3://test'}}) as ctx:
            # nuthatch operations use this config
            result = my_cached_function()
            # Check if config was accessed
            assert ctx.was_accessed

        # To reset config to normal disk-based loading:
        test_config.reset()
    """

    def __init__(self, config):
        self.config = config
        self.previous_provider = None
        self.was_accessed = False

    def __enter__(self):
        self.previous_provider = nuthatch.config._test_config_provider

        def tracking_provider():
            self.was_accessed = True
            return self.config

        nuthatch.config._test_config_provider = tracking_provider
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        nuthatch.config._test_config_provider = self.previous_provider
        return False

    @staticmethod
    def reset():
        """Reset to normal disk-based config loading."""
        nuthatch.config._test_config_provider = None


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
# Session-scoped Credential Fixtures
#
# These fixtures read credentials from environment variables.
# - Docker: env vars point to emulators (set by docker-compose.test.yml)
# - Local: env vars contain real cloud credentials (set by .envrc)
# =============================================================================

@pytest.fixture(scope="session")
def s3_credentials():
    """Get S3 credentials from environment.

    Environment variables:
    - AWS_ENDPOINT_URL: Custom S3 endpoint (e.g., LocalStack emulator)
    - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY: AWS credentials
    - AWS_DEFAULT_REGION: AWS region (defaults to us-east-1)
    """
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("S3 credentials not configured (set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)")

    return {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN"),
        "endpoint_url": os.environ.get("AWS_ENDPOINT_URL"),
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }


@pytest.fixture(scope="session")
def s3_test_bucket(s3_credentials):
    """Create a test bucket in S3/LocalStack."""
    import requests
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    bucket_name = f"nuthatch-test-{uuid.uuid4().hex[:8]}"
    endpoint = s3_credentials.get("endpoint_url", "https://s3.amazonaws.com")

    # Create AWS credentials for signing
    credentials = Credentials(
        access_key=s3_credentials["aws_access_key_id"],
        secret_key=s3_credentials["aws_secret_access_key"],
        token=s3_credentials.get("aws_session_token"),
    )

    # Create bucket via signed S3 REST API
    url = f"{endpoint}/{bucket_name}"
    request = AWSRequest(method="PUT", url=url)
    SigV4Auth(credentials, "s3", s3_credentials.get("region", "us-east-1")).add_auth(request)
    response = requests.put(url, headers=dict(request.headers))
    response.raise_for_status()

    yield bucket_name

    # Cleanup: delete all objects then the bucket
    try:
        # List and delete objects
        list_url = f"{endpoint}/{bucket_name}?list-type=2"
        request = AWSRequest(method="GET", url=list_url)
        SigV4Auth(credentials, "s3", s3_credentials.get("region", "us-east-1")).add_auth(request)
        response = requests.get(list_url, headers=dict(request.headers))

        if response.ok:
            # Parse XML response to get object keys
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            for content in root.findall(".//s3:Contents", ns):
                key = content.find("s3:Key", ns)
                if key is not None:
                    del_url = f"{endpoint}/{bucket_name}/{key.text}"
                    del_request = AWSRequest(method="DELETE", url=del_url)
                    SigV4Auth(credentials, "s3", s3_credentials.get("region", "us-east-1")).add_auth(del_request)
                    requests.delete(del_url, headers=dict(del_request.headers))

        # Delete bucket
        del_bucket_url = f"{endpoint}/{bucket_name}"
        request = AWSRequest(method="DELETE", url=del_bucket_url)
        SigV4Auth(credentials, "s3", s3_credentials.get("region", "us-east-1")).add_auth(request)
        requests.delete(del_bucket_url, headers=dict(request.headers))
    except Exception:
        pass


@pytest.fixture(scope="session")
def gcs_credentials():
    """Get GCS credentials from environment.

    Environment variables:
    - STORAGE_EMULATOR_HOST: GCS emulator endpoint (e.g., fake-gcs-server)
    - GOOGLE_APPLICATION_CREDENTIALS: Service account file path
    """
    endpoint = os.environ.get("STORAGE_EMULATOR_HOST")
    service_account_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if not service_account_file and not endpoint:
        raise RuntimeError("GCS credentials not configured (set GOOGLE_APPLICATION_CREDENTIALS or STORAGE_EMULATOR_HOST)")

    result = {"service_account_file": service_account_file}
    if endpoint:
        # Emulator mode
        result["endpoint_url"] = endpoint
        result["token"] = "anon"

    return result


@pytest.fixture(scope="session")
def gcs_test_bucket(gcs_credentials):
    """Create a test bucket in GCS/fake-gcs-server."""
    import requests
    import gcsfs

    bucket_name = f"nuthatch-test-{uuid.uuid4().hex[:8]}"
    endpoint = gcs_credentials.get("endpoint_url")

    if endpoint:
        # Create bucket via REST API for fake-gcs-server
        response = requests.post(
            f"{endpoint}/storage/v1/b",
            json={"name": bucket_name},
        )
        response.raise_for_status()
    else:
        # Create bucket via gcsfs for real GCS
        fs = gcsfs.GCSFileSystem()
        fs.mkdir(bucket_name)

    yield bucket_name

    # Cleanup
    try:
        if endpoint:
            requests.delete(f"{endpoint}/storage/v1/b/{bucket_name}")
        else:
            fs = gcsfs.GCSFileSystem()
            fs.rm(bucket_name, recursive=True)
    except Exception:
        pass


@pytest.fixture(scope="session")
def azure_credentials():
    """Get Azure credentials from environment.

    Environment variables:
    - AZURE_STORAGE_ACCOUNT: Azure storage account name
    - AZURE_STORAGE_KEY: Azure storage account key
    - AZURE_STORAGE_ENDPOINT: Custom endpoint (e.g., Azurite emulator)
    - AZURE_STORAGE_CONNECTION_STRING: Full connection string (alternative to above)
    """
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
    account_key = os.environ.get("AZURE_STORAGE_KEY")
    endpoint = os.environ.get("AZURE_STORAGE_ENDPOINT")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    if not account_name and not connection_string:
        raise RuntimeError("Azure credentials not configured (set AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_CONNECTION_STRING)")

    # Build connection string if not provided
    if not connection_string and account_name and account_key:
        if endpoint:
            # Emulator mode - parse host:port from endpoint
            from urllib.parse import urlparse
            parsed = urlparse(endpoint)
            host = parsed.hostname
            port = parsed.port or 10000
            connection_string = (
                f"DefaultEndpointsProtocol=http;"
                f"AccountName={account_name};"
                f"AccountKey={account_key};"
                f"BlobEndpoint=http://{host}:{port}/{account_name};"
            )
        else:
            # Real Azure
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={account_name};"
                f"AccountKey={account_key};"
                f"EndpointSuffix=core.windows.net"
            )

    result = {
        "account_name": account_name,
        "account_key": account_key,
        "connection_string": connection_string,
    }

    if endpoint:
        from urllib.parse import urlparse
        parsed = urlparse(endpoint)
        result["host"] = parsed.hostname
        result["port"] = parsed.port or 10000
        result["blob_endpoint"] = f"http://{parsed.hostname}:{parsed.port or 10000}/{account_name}"

    return result


@pytest.fixture(scope="session")
def azure_test_container(azure_credentials):
    """Create a test container in Azure/Azurite."""
    from azure.storage.blob import BlobServiceClient

    container_name = f"nuthatch-test-{uuid.uuid4().hex[:8]}"

    blob_service = BlobServiceClient.from_connection_string(
        azure_credentials["connection_string"]
    )
    blob_service.create_container(container_name)

    yield container_name

    try:
        blob_service.delete_container(container_name)
    except Exception:
        pass


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



def _write_config_to_disk(config):
    """Write nuthatch config to ~/.nuthatch.toml for CLI tests.

    The CLI reads config from disk, so we need to write the test config
    to ~/.nuthatch.toml for CLI commands to use the correct settings.
    """
    import tomli_w
    from pathlib import Path

    config_path = Path.home() / ".nuthatch.toml"

    # Convert config to the format expected in .nuthatch.toml
    # The file uses [tool.nuthatch] section
    toml_config = {"tool": {"nuthatch": config.get("root", {})}}
    if "local" in config:
        toml_config["tool"]["nuthatch"]["local"] = config["local"]

    with open(config_path, "wb") as f:
        tomli_w.dump(toml_config, f)

    return config_path


@pytest.fixture
def cloud_storage(request, tmp_path, monkeypatch):
    """
    Configure nuthatch for the parametrized cloud provider.

    The provider is determined by pytest_generate_tests based on markers.
    Configuration comes from environment variables set by either:
    - docker-compose.test.yml (emulators)
    - .envrc (real cloud credentials)

    Tests can either:
    1. Use the fixture's config directly (it's automatically set as the test config provider)
    2. Use the config dict to build their own config with test_config() context manager
    """
    # Use a temp directory as HOME to prevent overwriting real ~/.nuthatch.toml
    # This ensures tests are isolated from the user's actual config
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))

    provider = request.param

    # Use a unique cache directory per test to prevent test pollution
    # This is important because the bucket is session-scoped but tests should be isolated
    test_cache_id = uuid.uuid4().hex[:8]

    # Build config for this provider
    # Include 'local' for tests that use cache_local=True
    config = {
        'root': {'metadata_location': 'filesystem'},
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }

    if provider == "s3":
        s3_credentials = request.getfixturevalue("s3_credentials")
        s3_test_bucket = request.getfixturevalue("s3_test_bucket")
        config['root']['filesystem'] = f"s3://{s3_test_bucket}/cache-{test_cache_id}"

        fs_options = {
            'key': s3_credentials["aws_access_key_id"],
            'secret': s3_credentials["aws_secret_access_key"],
        }
        if s3_credentials.get("aws_session_token"):
            fs_options['token'] = s3_credentials["aws_session_token"]
        if s3_credentials.get("endpoint_url"):
            fs_options['client_kwargs'] = {'endpoint_url': s3_credentials["endpoint_url"]}
        config['root']['filesystem_options'] = fs_options

    elif provider == "gcs":
        gcs_credentials = request.getfixturevalue("gcs_credentials")
        gcs_test_bucket = request.getfixturevalue("gcs_test_bucket")
        config['root']['filesystem'] = f"gs://{gcs_test_bucket}/cache-{test_cache_id}"

        fs_options = {}
        if gcs_credentials.get("endpoint_url"):
            # Emulator mode
            fs_options['token'] = 'anon'
            fs_options['endpoint_url'] = gcs_credentials["endpoint_url"]
            fs_options['service_account_file'] = gcs_credentials["service_account_file"]
            monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_credentials["endpoint_url"])
        else:
            # Real GCS - use default credentials
            fs_options['service_account_file'] = gcs_credentials.get("service_account_file")
        config['root']['filesystem_options'] = fs_options

    elif provider == "azure":
        azure_credentials = request.getfixturevalue("azure_credentials")
        azure_test_container = request.getfixturevalue("azure_test_container")
        config['root']['filesystem'] = f"abfs://{azure_test_container}/cache-{test_cache_id}"

        fs_options = {
            'account_name': azure_credentials.get("account_name"),
            'account_key': azure_credentials.get("account_key"),
            'connection_string': azure_credentials["connection_string"],
        }
        if azure_credentials.get("host"):
            # Emulator mode - use account_host for fsspec/adlfs
            fs_options['account_host'] = f"{azure_credentials['host']}:{azure_credentials['port']}"
            # delta-rs needs explicit endpoint URL (NOT azure_storage_use_emulator which defaults to localhost)
            fs_options['azure_storage_endpoint'] = azure_credentials['blob_endpoint']
            fs_options['allow_http'] = 'true'
        config['root']['filesystem_options'] = fs_options

    # Track whether config was explicitly accessed (for tests that derive their own config)
    config_dict_accessed = []

    class CloudStorageResult(dict):
        """Result object from cloud_storage fixture with config context support."""

        def __getitem__(self, key):
            if key == "config":
                config_dict_accessed.append(True)
            return super().__getitem__(key)

        def config_context(self, custom_config):
            """Return a context manager for using a custom config.

            Use this when you need to override the default config, e.g. for
            testing mirror storage with multiple config phases.

            Usage:
                with cloud_storage.config_context(my_config):
                    # nuthatch operations use my_config
                    result = my_cached_function()
            """
            config_dict_accessed.append(True)  # Accessing config_context counts as usage
            return test_config(custom_config)

    result = CloudStorageResult({
        "provider": provider,
        "config": config,
    })

    # Use context manager for clean setup/teardown with access tracking
    with test_config(config) as ctx:
        # Write config to disk so CLI commands can read it
        # Do this INSIDE the context after test_config is set
        _write_config_to_disk(config)
        yield result

    # Verify the test actually used cloud storage
    # Valid usage: either the config provider was invoked by nuthatch operations,
    # OR the test explicitly accessed cloud_storage["config"] to derive its own config
    if not ctx.was_accessed and not config_dict_accessed:
        present_markers = []
        for marker in ["s3", "gcs", "azure"]:
            if request.node.get_closest_marker(marker):
                present_markers.append(f"@pytest.mark.{marker}")
        markers_str = ", ".join(present_markers)
        pytest.fail(
            f"Test requested cloud_storage fixture but never accessed cloud storage config. "
            f"Remove cloud provider markers ({markers_str}) if this test doesn't need cloud storage."
        )


# =============================================================================
# Explicit cloud storage fixtures (for tests that want direct access)
# =============================================================================

@pytest.fixture
def s3_storage(request, s3_credentials, s3_test_bucket, tmp_path):
    """Configure nuthatch to use S3/LocalStack."""
    filesystem = f"s3://{s3_test_bucket}/cache"

    fs_options = {
        'key': s3_credentials["aws_access_key_id"],
        'secret': s3_credentials["aws_secret_access_key"],
    }
    if s3_credentials.get("aws_session_token"):
        fs_options['token'] = s3_credentials["aws_session_token"]
    if s3_credentials.get("endpoint_url"):
        fs_options['client_kwargs'] = {'endpoint_url': s3_credentials["endpoint_url"]}

    config = {
        'root': {
            'filesystem': filesystem,
            'metadata_location': 'filesystem',
            'filesystem_options': fs_options,
        },
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }

    with test_config(config):
        yield {
            "uri": filesystem,
            "bucket": s3_test_bucket,
            "credentials": s3_credentials,
            "provider": "s3",
        }


@pytest.fixture
def gcs_storage(request, gcs_credentials, gcs_test_bucket, monkeypatch, tmp_path):
    """Configure nuthatch to use GCS/fake-gcs-server."""
    filesystem = f"gs://{gcs_test_bucket}/cache"

    fs_options = {}
    if gcs_credentials.get("endpoint_url"):
        fs_options['token'] = 'anon'
        fs_options['endpoint_url'] = gcs_credentials["endpoint_url"]
        fs_options['service_account_file'] = gcs_credentials["service_account_file"]
        monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_credentials["endpoint_url"])
    else:
        fs_options['service_account_file'] = gcs_credentials.get("service_account_file")

    config = {
        'root': {
            'filesystem': filesystem,
            'metadata_location': 'filesystem',
            'filesystem_options': fs_options,
        },
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }

    with test_config(config):
        yield {
            "uri": filesystem,
            "bucket": gcs_test_bucket,
            "credentials": gcs_credentials,
            "provider": "gcs",
        }


@pytest.fixture
def azure_storage(request, azure_credentials, azure_test_container, tmp_path):
    """Configure nuthatch to use Azure/Azurite."""
    filesystem = f"abfs://{azure_test_container}/cache"

    fs_options = {
        'account_name': azure_credentials.get("account_name"),
        'account_key': azure_credentials.get("account_key"),
        'connection_string': azure_credentials["connection_string"],
    }
    if azure_credentials.get("host"):
        # Emulator mode - use account_host for fsspec/adlfs
        fs_options['account_host'] = f"{azure_credentials['host']}:{azure_credentials['port']}"
        # delta-rs needs explicit endpoint URL (NOT azure_storage_use_emulator which defaults to localhost)
        fs_options['azure_storage_endpoint'] = azure_credentials['blob_endpoint']
        fs_options['allow_http'] = 'true'

    config = {
        'root': {
            'filesystem': filesystem,
            'metadata_location': 'filesystem',
            'filesystem_options': fs_options,
        },
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }

    with test_config(config):
        yield {
            "uri": filesystem,
            "container": azure_test_container,
            "credentials": azure_credentials,
            "provider": "azure",
        }


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
        raise RuntimeError("PostgreSQL not configured (set POSTGRES_HOST)")

    return {
        "driver": "postgresql",
        "username": os.environ.get("POSTGRES_USER", "test"),
        "password": os.environ.get("POSTGRES_PASSWORD", "test"),
        "host": host,
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "nuthatch_test"),
    }


@pytest.fixture
def postgres_storage(request, postgres_credentials, tmp_path):
    """Configure nuthatch to use PostgreSQL for SQL backend."""
    config = {
        'root': {
            'filesystem': str(tmp_path / 'nuthatch_cache'),
            'metadata_location': 'filesystem',
            'sql': postgres_credentials,
        }
    }

    with test_config(config):
        yield {"credentials": postgres_credentials, "config": config}
