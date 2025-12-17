"""
Pytest fixtures for cloud storage testing with testcontainers.

This module provides fixtures for testing nuthatch backends against
LocalStack (S3), fake-gcs-server (GCS), and Azurite (Azure Blob Storage).

When cloud-test dependencies are installed, ALL tests automatically run 3 times -
once for each cloud provider. No test modifications needed.

Usage:
    pytest                        # Run all tests against all 3 cloud providers
    pytest --cloud-provider s3    # Run only against S3
    pytest --cloud-provider gcs   # Run only against GCS
    pytest --cloud-provider azure # Run only against Azure
    pytest --no-cloud             # Run without cloud testing (use pyproject.toml config)
"""

import os
import uuid
import pytest

# Check if testcontainers dependencies are available
try:
    from testcontainers.localstack import LocalStackContainer
    from testcontainers.core.container import DockerContainer
    import s3fs  # noqa: F401 - used in fixtures
    CLOUD_DEPS_AVAILABLE = True
except ImportError:
    CLOUD_DEPS_AVAILABLE = False


def pytest_addoption(parser):
    """Add command-line options for cloud testing."""
    parser.addoption(
        "--cloud-provider",
        action="store",
        default=None,
        choices=["s3", "gcs", "azure"],
        help="Run tests against a specific cloud provider only",
    )
    parser.addoption(
        "--no-cloud",
        action="store_true",
        default=False,
        help="Skip cloud testing, use default config",
    )


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "cloud: marks tests as requiring cloud storage containers")
    config.addinivalue_line("markers", "s3: marks tests as requiring S3/LocalStack")
    config.addinivalue_line("markers", "gcs: marks tests as requiring GCS/fake-gcs-server")
    config.addinivalue_line("markers", "azure: marks tests as requiring Azure/Azurite")
    config.addinivalue_line("markers", "no_cloud: marks tests to skip cloud provider parametrization")


# =============================================================================
# Session-scoped Container Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def localstack_container(request):
    """Start a LocalStack container for S3 testing."""
    if not CLOUD_DEPS_AVAILABLE or request.config.getoption("--no-cloud"):
        pytest.skip("Cloud testing disabled")

    container = LocalStackContainer(image="localstack/localstack:latest")
    container.with_services("s3")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def s3_credentials(localstack_container):
    """Get S3 credentials for LocalStack."""
    return {
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "endpoint_url": localstack_container.get_url(),
        "region_name": "us-east-1",
    }


@pytest.fixture(scope="session")
def s3_test_bucket(localstack_container, s3_credentials):
    """Create a test bucket in LocalStack S3."""
    bucket_name = f"nuthatch-test-{uuid.uuid4().hex[:8]}"

    fs = s3fs.S3FileSystem(
        key=s3_credentials["aws_access_key_id"],
        secret=s3_credentials["aws_secret_access_key"],
        endpoint_url=s3_credentials["endpoint_url"],
    )
    fs.mkdir(bucket_name)

    yield bucket_name

    # Cleanup
    try:
        fs.rm(bucket_name, recursive=True)
    except Exception:
        pass


@pytest.fixture(scope="session")
def gcs_container(request):
    """Start a fake-gcs-server container for GCS testing.

    Uses fake-gcs-server-xml which has XML API support (list-type=2 and multipart uploads)
    required by delta-rs/object_store.
    """
    if not CLOUD_DEPS_AVAILABLE or request.config.getoption("--no-cloud"):
        pytest.skip("Cloud testing disabled")

    # Use a fixed external port so fake-gcs-server can generate correct URLs
    import socket
    # Find an available port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        external_port = s.getsockname()[1]

    # Use fake-gcs-server with XML API support for delta-rs compatibility
    # Published from: https://github.com/rhiza-research/fake-gcs-server
    container = DockerContainer("ghcr.io/rhiza-research/fake-gcs-server:latest")
    container.with_bind_ports(4443, external_port)
    container.with_command(f"-scheme http -backend memory -public-host localhost:{external_port} -external-url http://localhost:{external_port}")
    container.start()

    # Wait for server to be ready
    import time
    import requests
    endpoint = f"http://localhost:{external_port}"

    for _ in range(30):
        try:
            requests.get(f"{endpoint}/storage/v1/b", timeout=1)
            break
        except Exception:
            time.sleep(1)

    # Store the endpoint for later use
    container._external_endpoint = endpoint

    yield container

    container.stop()


@pytest.fixture(scope="session")
def gcs_credentials(gcs_container, tmp_path_factory):
    """Get GCS credentials for fake-gcs-server.

    Creates a fake service account JSON file with gcs_base_url pointing to the emulator.
    Uses disable_oauth=true to skip authentication (matching arrow-rs CI config).
    See: https://github.com/apache/arrow-rs-object-store/blob/main/.github/workflows/ci.yml
    """
    import json

    endpoint = gcs_container._external_endpoint

    # Use minimal credentials with disable_oauth=true (matches arrow-rs CI)
    # object_store will skip authentication when disable_oauth is true
    fake_creds = {
        "gcs_base_url": endpoint,
        "disable_oauth": True,
        "client_email": "",
        "private_key": "",
        "private_key_id": "",
    }

    creds_dir = tmp_path_factory.mktemp("gcs_creds")
    creds_file = creds_dir / "fake_service_account.json"
    creds_file.write_text(json.dumps(fake_creds))

    return {
        "endpoint_url": endpoint,
        "token": "anon",  # For gcsfs/fsspec
        "service_account_file": str(creds_file),  # For delta-rs/object_store
    }


@pytest.fixture(scope="session")
def gcs_test_bucket(gcs_container, gcs_credentials):
    """Create a test bucket in fake-gcs-server."""
    import requests

    bucket_name = f"nuthatch-test-{uuid.uuid4().hex[:8]}"
    endpoint = gcs_credentials["endpoint_url"]

    response = requests.post(
        f"{endpoint}/storage/v1/b",
        json={"name": bucket_name},
    )
    response.raise_for_status()

    yield bucket_name

# https://learn.microsoft.com/en-us/azure/storage/common/storage-connect-azurite?tabs=blob-storage#use-a-well-known-storage-account-and-key
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


@pytest.fixture(scope="session")
def azurite_container(request):
    """Start an Azurite container for Azure Blob Storage testing."""
    if not CLOUD_DEPS_AVAILABLE or request.config.getoption("--no-cloud"):
        pytest.skip("Cloud testing disabled")

    # Bind to standard Azurite port 10000 so delta-rs emulator mode works
    # delta-rs azure_storage_use_emulator expects 127.0.0.1:10000
    container = DockerContainer("mcr.microsoft.com/azure-storage/azurite:latest")
    container.with_bind_ports(10000, 10000)
    container.with_command("azurite-blob --blobHost 0.0.0.0 --blobPort 10000")
    container.start()

    # Wait for Azurite to be ready
    import time
    import socket
    host = "127.0.0.1"
    port = 10000

    for _ in range(30):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((host, port))
            sock.close()
            break
        except Exception:
            time.sleep(1)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def azure_credentials(azurite_container):
    """Get Azure credentials for Azurite."""
    # Use fixed port 10000 for delta-rs emulator mode compatibility
    host = "127.0.0.1"
    port = 10000

    connection_string = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={AZURITE_ACCOUNT_NAME};"
        f"AccountKey={AZURITE_ACCOUNT_KEY};"
        f"BlobEndpoint=http://{host}:{port}/{AZURITE_ACCOUNT_NAME};"
    )

    return {
        "account_name": AZURITE_ACCOUNT_NAME,
        "account_key": AZURITE_ACCOUNT_KEY,
        "connection_string": connection_string,
        "blob_endpoint": f"http://{host}:{port}/{AZURITE_ACCOUNT_NAME}",
        "host": host,
        "port": port,
    }


@pytest.fixture(scope="session")
def azure_test_container(azurite_container, azure_credentials):
    """Create a test container in Azurite."""
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
# Auto-use fixture that runs ALL tests against each cloud provider
# =============================================================================

def get_cloud_providers(config):
    """Determine which cloud providers to test against."""
    if not CLOUD_DEPS_AVAILABLE:
        return []
    if config.getoption("--no-cloud"):
        return []

    specific = config.getoption("--cloud-provider")
    if specific:
        return [specific]

    return ["s3", "gcs", "azure"]


def pytest_generate_tests(metafunc):
    """Parametrize all tests to run against each cloud provider."""
    # Skip tests marked with @pytest.mark.no_cloud
    if metafunc.definition.get_closest_marker("no_cloud"):
        print(f"\n>>> pytest_generate_tests: SKIPPING {metafunc.function.__name__} (marked no_cloud)")
        return

    providers = get_cloud_providers(metafunc.config)
    print(f"\n>>> pytest_generate_tests: {metafunc.function.__name__}, providers={providers}, CLOUD_DEPS={CLOUD_DEPS_AVAILABLE}")

    if providers:
        if "cloud_provider" not in metafunc.fixturenames:
            # Add cloud_provider parameter to all tests
            metafunc.fixturenames.append("cloud_provider")
            print(f">>> Added cloud_provider to {metafunc.function.__name__}")
        # Always parametrize (even if cloud_provider was explicitly requested)
        metafunc.parametrize("cloud_provider", providers, indirect=True, scope="function")
        print(f">>> Parametrized {metafunc.function.__name__} with {providers}")


# Store container info at session level for use in cloud_provider fixture
_session_container_info = {}


@pytest.fixture(scope="session")
def setup_cloud_containers(
    localstack_container,
    s3_credentials,
    s3_test_bucket,
    gcs_container,
    gcs_credentials,
    gcs_test_bucket,
    azurite_container,
    azure_credentials,
    azure_test_container,
):
    """Session fixture that ensures all containers are ready and stores their info."""
    _session_container_info["s3"] = {
        "bucket": s3_test_bucket,
        "credentials": s3_credentials,
    }
    _session_container_info["gcs"] = {
        "bucket": gcs_test_bucket,
        "credentials": gcs_credentials,
    }
    _session_container_info["azure"] = {
        "container": azure_test_container,
        "credentials": azure_credentials,
    }
    return _session_container_info


@pytest.fixture(autouse=True)
def _force_cloud_provider(request):
    """Autouse fixture that ensures cloud_provider fixture runs when parametrized.

    pytest_generate_tests parametrizes tests with cloud_provider, but the fixture
    won't be invoked unless explicitly requested. This autouse fixture forces it.
    """
    # Skip for tests marked with no_cloud
    if request.node.get_closest_marker("no_cloud"):
        return

    providers = get_cloud_providers(request.config)
    if providers:
        # Cloud testing is active - force the cloud_provider fixture to run
        print(f"\n>>> _force_cloud_provider: invoking cloud_provider fixture")
        try:
            val = request.getfixturevalue("cloud_provider")
            print(f">>> _force_cloud_provider: got value {val}")
        except Exception as e:
            print(f">>> _force_cloud_provider: EXCEPTION {e}")
            raise


@pytest.fixture
def cloud_provider(request, setup_cloud_containers, monkeypatch, tmp_path):
    """
    Configure nuthatch to use the specified cloud provider.

    This fixture is automatically applied to all tests when cloud deps are available.
    Uses set_test_config_provider to bypass disk config loading entirely.
    """
    provider = request.param
    info = setup_cloud_containers

    from nuthatch.config import set_test_config_provider

    # Build config for this provider
    # Include 'local' for tests that use cache_local=True
    config = {
        'root': {'metadata_location': 'filesystem'},
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }

    if provider == "s3":
        s3_info = info["s3"]
        config['root']['filesystem'] = f"s3://{s3_info['bucket']}/cache"
        config['root']['filesystem_options'] = {
            'key': s3_info["credentials"]["aws_access_key_id"],
            'secret': s3_info["credentials"]["aws_secret_access_key"],
            'client_kwargs': {
                'endpoint_url': s3_info["credentials"]["endpoint_url"],
            },
        }

    elif provider == "gcs":
        gcs_info = info["gcs"]
        config['root']['filesystem'] = f"gs://{gcs_info['bucket']}/cache"
        config['root']['filesystem_options'] = {
            'token': 'anon',  # For gcsfs/fsspec
            'endpoint_url': gcs_info["credentials"]["endpoint_url"],  # For gcsfs custom endpoint
            # Service account file with gcs_base_url for delta-rs/object_store
            'service_account_file': gcs_info["credentials"]["service_account_file"],
        }
        monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_info["credentials"]["endpoint_url"])

    elif provider == "azure":
        azure_info = info["azure"]
        # Use abfs:// (not az://) for delta-rs compatibility with Azurite
        # delta-rs interprets az:// paths incorrectly, writing to container root
        config['root']['filesystem'] = f"abfs://{azure_info['container']}/cache"
        config['root']['filesystem_options'] = {
            'account_name': azure_info["credentials"]["account_name"],
            'account_key': azure_info["credentials"]["account_key"],
            'account_host': f"{azure_info['credentials']['host']}:{azure_info['credentials']['port']}",
            'connection_string': azure_info["credentials"]["connection_string"],
            # For delta-rs: use Azurite emulator (expects 127.0.0.1:10000)
            'azure_storage_use_emulator': '1',
        }

    # Use test config provider to bypass disk loading
    set_test_config_provider(lambda: config)
    print(f">>> cloud_provider: set test config provider with {config}")

    yield provider

    # Reset to normal disk-based config loading
    set_test_config_provider(None)


# =============================================================================
# Explicit cloud storage fixtures (for tests that want direct access)
# =============================================================================

@pytest.fixture
def s3_storage(s3_credentials, s3_test_bucket):
    """Configure nuthatch to use LocalStack S3."""
    from nuthatch.config import set_test_config_provider

    filesystem = f"s3://{s3_test_bucket}/cache"
    config = {
        'root': {
            'filesystem': filesystem,
            'metadata_location': 'filesystem',
            'filesystem_options': {
                'key': s3_credentials["aws_access_key_id"],
                'secret': s3_credentials["aws_secret_access_key"],
                'client_kwargs': {
                    'endpoint_url': s3_credentials["endpoint_url"],
                },
            },
        }
    }
    set_test_config_provider(lambda: config)

    yield {
        "uri": filesystem,
        "bucket": s3_test_bucket,
        "credentials": s3_credentials,
        "provider": "s3",
    }

    set_test_config_provider(None)


@pytest.fixture
def gcs_storage(gcs_credentials, gcs_test_bucket, monkeypatch):
    """Configure nuthatch to use fake-gcs-server."""
    from nuthatch.config import set_test_config_provider

    filesystem = f"gs://{gcs_test_bucket}/cache"
    config = {
        'root': {
            'filesystem': filesystem,
            'metadata_location': 'filesystem',
            'filesystem_options': {
                'token': 'anon',  # For gcsfs/fsspec
                'service_account_file': gcs_credentials["service_account_file"],  # For delta-rs
            },
        }
    }
    set_test_config_provider(lambda: config)
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_credentials["endpoint_url"])

    yield {
        "uri": filesystem,
        "bucket": gcs_test_bucket,
        "credentials": gcs_credentials,
        "provider": "gcs",
    }

    set_test_config_provider(None)


@pytest.fixture
def azure_storage(azure_credentials, azure_test_container):
    """Configure nuthatch to use Azurite."""
    from nuthatch.config import set_test_config_provider

    # Use abfs:// (not az://) for delta-rs compatibility with Azurite
    filesystem = f"abfs://{azure_test_container}/cache"
    config = {
        'root': {
            'filesystem': filesystem,
            'metadata_location': 'filesystem',
            'filesystem_options': {
                'account_name': azure_credentials["account_name"],
                'account_key': azure_credentials["account_key"],
                'account_host': f"{azure_credentials['host']}:{azure_credentials['port']}",
                'connection_string': azure_credentials["connection_string"],
            },
        }
    }
    set_test_config_provider(lambda: config)

    yield {
        "uri": filesystem,
        "container": azure_test_container,
        "credentials": azure_credentials,
        "provider": "azure",
    }

    set_test_config_provider(None)
