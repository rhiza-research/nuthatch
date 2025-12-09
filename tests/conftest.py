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
from pathlib import Path

# Path to test config file - nuthatch will find this before pyproject.toml
TEST_CONFIG_PATH = Path(__file__).parent / "nuthatch.toml"

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


def pytest_unconfigure(config):
    """Clean up test config file after test session."""
    if TEST_CONFIG_PATH.exists():
        TEST_CONFIG_PATH.unlink()


# =============================================================================
# Session-scoped Container Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def localstack_container(request):
    """Start a LocalStack container for S3 testing."""
    if not CLOUD_DEPS_AVAILABLE or request.config.getoption("--no-cloud"):
        pytest.skip("Cloud testing disabled")

    container = LocalStackContainer(image="localstack/localstack:3.0")
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
    """Start a fake-gcs-server container for GCS testing."""
    if not CLOUD_DEPS_AVAILABLE or request.config.getoption("--no-cloud"):
        pytest.skip("Cloud testing disabled")

    container = DockerContainer("fsouza/fake-gcs-server:latest")
    container.with_exposed_ports(4443)
    container.with_command("-scheme http -public-host localhost")
    container.start()

    # Wait for server to be ready
    import time
    import requests
    host = container.get_container_host_ip()
    port = container.get_exposed_port(4443)
    endpoint = f"http://{host}:{port}"

    for _ in range(30):
        try:
            requests.get(f"{endpoint}/storage/v1/b", timeout=1)
            break
        except Exception:
            time.sleep(1)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def gcs_credentials(gcs_container):
    """Get GCS credentials for fake-gcs-server."""
    host = gcs_container.get_container_host_ip()
    port = gcs_container.get_exposed_port(4443)
    return {
        "endpoint_url": f"http://{host}:{port}",
        "token": "anon",
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

    container = DockerContainer("mcr.microsoft.com/azure-storage/azurite:latest")
    container.with_exposed_ports(10000)
    container.with_command("azurite-blob --blobHost 0.0.0.0 --blobPort 10000")
    container.start()

    # Wait for Azurite to be ready
    import time
    import socket
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(10000))

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
    host = azurite_container.get_container_host_ip()
    port = azurite_container.get_exposed_port(10000)

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


def write_test_config(filesystem: str):
    """Write a nuthatch.toml config file for testing.

    This file is placed in the tests/ directory so nuthatch finds it
    before the root pyproject.toml.
    """
    print(f"\n>>> WRITING TEST CONFIG: {TEST_CONFIG_PATH}")
    print(f">>> FILESYSTEM: {filesystem}")
    config_content = f'''# Auto-generated test config - DO NOT EDIT
# This file is created by conftest.py and deleted after tests

[nuthatch]
filesystem = "{filesystem}"
metadata_location = "filesystem"
'''
    TEST_CONFIG_PATH.write_text(config_content)
    print(f">>> FILE EXISTS: {TEST_CONFIG_PATH.exists()}")


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
def cloud_provider(request, setup_cloud_containers, monkeypatch):
    """
    Configure nuthatch to use the specified cloud provider.

    This fixture is automatically applied to all tests when cloud deps are available.
    Sets dynamic_parameters directly using the test module's name as the key.
    """
    provider = request.param
    info = setup_cloud_containers

    import nuthatch.config
    nuthatch.config.dynamic_parameters.clear()

    # Get the test module name - this is the key nuthatch.config uses to look up dynamic params
    test_module = request.module.__name__.partition('.')[0]

    # Build config for this provider
    # Include metadata_location since we're replacing the entire root dict
    config = {'root': {'metadata_location': 'filesystem'}}

    if provider == "s3":
        s3_info = info["s3"]
        config['root']['filesystem'] = f"s3://{s3_info['bucket']}/cache"
        config['root']['filesystem_options'] = {
            'key': s3_info["credentials"]["aws_access_key_id"],
            'secret': s3_info["credentials"]["aws_secret_access_key"],
            'endpoint_url': s3_info["credentials"]["endpoint_url"],
        }

    elif provider == "gcs":
        gcs_info = info["gcs"]
        config['root']['filesystem'] = f"gs://{gcs_info['bucket']}/cache"
        monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_info["credentials"]["endpoint_url"])

    elif provider == "azure":
        azure_info = info["azure"]
        config['root']['filesystem'] = f"az://{azure_info['container']}/cache"
        config['root']['filesystem_options'] = {
            'account_name': azure_info["credentials"]["account_name"],
            'account_key': azure_info["credentials"]["account_key"],
            'account_host': f"{azure_info['credentials']['host']}:{azure_info['credentials']['port']}",
            'connection_string': azure_info["credentials"]["connection_string"],
        }

    # Set directly in dynamic_parameters using the test module's name
    nuthatch.config.dynamic_parameters[test_module] = config
    print(f">>> cloud_provider: set dynamic_parameters['{test_module}'] = {config}")

    yield provider

    # Clean up dynamic parameters after test
    nuthatch.config.dynamic_parameters.clear()


# =============================================================================
# Explicit cloud storage fixtures (for tests that want direct access)
# =============================================================================

@pytest.fixture
def s3_storage(s3_credentials, s3_test_bucket, monkeypatch):
    """Configure nuthatch to use LocalStack S3."""
    filesystem = f"s3://{s3_test_bucket}/cache"
    write_test_config(filesystem)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", s3_credentials["aws_access_key_id"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", s3_credentials["aws_secret_access_key"])
    monkeypatch.setenv("AWS_ENDPOINT_URL", s3_credentials["endpoint_url"])
    monkeypatch.setenv("AWS_DEFAULT_REGION", s3_credentials["region_name"])

    yield {
        "uri": filesystem,
        "bucket": s3_test_bucket,
        "credentials": s3_credentials,
        "provider": "s3",
    }

    if TEST_CONFIG_PATH.exists():
        TEST_CONFIG_PATH.unlink()


@pytest.fixture
def gcs_storage(gcs_credentials, gcs_test_bucket, monkeypatch):
    """Configure nuthatch to use fake-gcs-server."""
    filesystem = f"gs://{gcs_test_bucket}/cache"
    write_test_config(filesystem)
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_credentials["endpoint_url"])

    yield {
        "uri": filesystem,
        "bucket": gcs_test_bucket,
        "credentials": gcs_credentials,
        "provider": "gcs",
    }

    if TEST_CONFIG_PATH.exists():
        TEST_CONFIG_PATH.unlink()


@pytest.fixture
def azure_storage(azure_credentials, azure_test_container, monkeypatch):
    """Configure nuthatch to use Azurite."""
    filesystem = f"az://{azure_test_container}/cache"
    write_test_config(filesystem)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", azure_credentials["account_name"])
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", azure_credentials["account_key"])
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", azure_credentials["connection_string"])

    yield {
        "uri": filesystem,
        "container": azure_test_container,
        "credentials": azure_credentials,
        "provider": "azure",
    }

    if TEST_CONFIG_PATH.exists():
        TEST_CONFIG_PATH.unlink()
