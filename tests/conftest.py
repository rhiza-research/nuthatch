"""
Pytest fixtures for cloud storage testing with testcontainers.

This module provides fixtures for testing nuthatch backends against
LocalStack (S3), fake-gcs-server (GCS), and Azurite (Azure Blob Storage).

Tests must explicitly:
1. Mark which provider(s) they support: @pytest.mark.s3, @pytest.mark.gcs, @pytest.mark.azure
2. Request the cloud_storage fixture

Usage:
    pytest                          # Run against local containers
    pytest -m integration           # Run against live cloud services
    pytest -m s3                    # Run only S3 tests
    pytest -m "gcs and integration" # Run GCS tests against live GCS
"""

import os
import uuid
import pytest

from testcontainers.localstack import LocalStackContainer
from testcontainers.core.container import DockerContainer
import s3fs


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "s3: marks tests as requiring S3/LocalStack")
    config.addinivalue_line("markers", "gcs: marks tests as requiring GCS/fake-gcs-server")
    config.addinivalue_line("markers", "azure: marks tests as requiring Azure/Azurite")
    config.addinivalue_line("markers", "integration: run against live cloud (not local containers)")


def pytest_generate_tests(metafunc):
    """Parametrize cloud_storage based on which provider markers are on the test."""
    if "cloud_storage" not in metafunc.fixturenames:
        return

    # Check which provider markers are on this test
    providers = []
    if metafunc.definition.get_closest_marker("s3"):
        providers.append("s3")
    if metafunc.definition.get_closest_marker("gcs"):
        providers.append("gcs")
    if metafunc.definition.get_closest_marker("azure"):
        providers.append("azure")

    if not providers:
        pytest.fail(f"Test {metafunc.function.__name__} requests cloud_storage but has no provider markers (@pytest.mark.s3, @pytest.mark.gcs, @pytest.mark.azure)")

    metafunc.parametrize("cloud_storage", providers, indirect=True)


def is_integration_mode(request):
    """Check if running in integration mode (live cloud, not containers)."""
    return request.node.get_closest_marker("integration") is not None


# =============================================================================
# Session-scoped Container Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def localstack_container():
    """Start a LocalStack container for S3 testing."""
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
def gcs_container():
    """Start a fake-gcs-server container for GCS testing.

    Uses fake-gcs-server-xml which has XML API support (list-type=2 and multipart uploads)
    required by delta-rs/object_store.
    """
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
def azurite_container():
    """Start an Azurite container for Azure Blob Storage testing."""
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

    The provider is determined by pytest_generate_tests based on markers.
    Local mode: uses containers (LocalStack, fake-gcs-server, Azurite)
    Integration mode (-m integration): uses live cloud credentials from env
    """
    from nuthatch.config import set_test_config_provider

    provider = request.param
    integration = is_integration_mode(request)

    # Build config for this provider
    # Include 'local' for tests that use cache_local=True
    config = {
        'root': {'metadata_location': 'filesystem'},
        'local': {'filesystem': str(tmp_path / 'local_cache')},
    }

    if provider == "s3":
        if integration:
            bucket = os.environ.get("AWS_TEST_BUCKET")
            if not bucket:
                pytest.skip("AWS_TEST_BUCKET not set for integration test")
            config['root']['filesystem'] = f"s3://{bucket}/nuthatch-test-{uuid.uuid4().hex[:8]}"
            config['root']['filesystem_options'] = {
                'key': os.environ.get("AWS_ACCESS_KEY_ID"),
                'secret': os.environ.get("AWS_SECRET_ACCESS_KEY"),
            }
        else:
            s3_credentials = request.getfixturevalue("s3_credentials")
            s3_test_bucket = request.getfixturevalue("s3_test_bucket")
            config['root']['filesystem'] = f"s3://{s3_test_bucket}/cache"
            config['root']['filesystem_options'] = {
                'key': s3_credentials["aws_access_key_id"],
                'secret': s3_credentials["aws_secret_access_key"],
                'client_kwargs': {
                    'endpoint_url': s3_credentials["endpoint_url"],
                },
            }

    elif provider == "gcs":
        if integration:
            bucket = os.environ.get("GCS_TEST_BUCKET")
            if not bucket:
                pytest.skip("GCS_TEST_BUCKET not set for integration test")
            config['root']['filesystem'] = f"gs://{bucket}/nuthatch-test-{uuid.uuid4().hex[:8]}"
            config['root']['filesystem_options'] = {}
        else:
            gcs_credentials = request.getfixturevalue("gcs_credentials")
            gcs_test_bucket = request.getfixturevalue("gcs_test_bucket")
            config['root']['filesystem'] = f"gs://{gcs_test_bucket}/cache"
            config['root']['filesystem_options'] = {
                'token': 'anon',  # For gcsfs/fsspec
                'endpoint_url': gcs_credentials["endpoint_url"],  # For gcsfs custom endpoint
                # Service account file with gcs_base_url for delta-rs/object_store
                'service_account_file': gcs_credentials["service_account_file"],
            }
            monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_credentials["endpoint_url"])

    elif provider == "azure":
        if integration:
            container = os.environ.get("AZURE_TEST_CONTAINER")
            if not container:
                pytest.skip("AZURE_TEST_CONTAINER not set for integration test")
            config['root']['filesystem'] = f"abfs://{container}/nuthatch-test-{uuid.uuid4().hex[:8]}"
            config['root']['filesystem_options'] = {
                'connection_string': os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
            }
        else:
            azure_credentials = request.getfixturevalue("azure_credentials")
            azure_test_container = request.getfixturevalue("azure_test_container")
            # Use abfs:// (not az://) for delta-rs compatibility with Azurite
            # delta-rs interprets az:// paths incorrectly, writing to container root
            config['root']['filesystem'] = f"abfs://{azure_test_container}/cache"
            config['root']['filesystem_options'] = {
                'account_name': azure_credentials["account_name"],
                'account_key': azure_credentials["account_key"],
                'account_host': f"{azure_credentials['host']}:{azure_credentials['port']}",
                'connection_string': azure_credentials["connection_string"],
                # For delta-rs: use Azurite emulator (expects 127.0.0.1:10000)
                'azure_storage_use_emulator': '1',
            }

    # Track whether the config provider was actually accessed during the test
    config_accessed = []

    def config_provider():
        config_accessed.append(True)
        return config

    # Use test config provider to bypass disk loading
    set_test_config_provider(config_provider)

    yield {"provider": provider, "config": config, "integration": integration}

    # Reset to normal disk-based config loading
    set_test_config_provider(None)

    # Verify the test actually used cloud storage
    if not config_accessed:
        # Find which markers are actually on this test
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
    """Configure nuthatch to use LocalStack S3."""
    from nuthatch.config import set_test_config_provider

    if is_integration_mode(request):
        bucket = os.environ.get("AWS_TEST_BUCKET")
        if not bucket:
            pytest.skip("AWS_TEST_BUCKET not set for integration test")
        filesystem = f"s3://{bucket}/nuthatch-test-{uuid.uuid4().hex[:8]}"
        config = {
            'root': {
                'filesystem': filesystem,
                'metadata_location': 'filesystem',
                'filesystem_options': {
                    'key': os.environ.get("AWS_ACCESS_KEY_ID"),
                    'secret': os.environ.get("AWS_SECRET_ACCESS_KEY"),
                },
            },
            'local': {'filesystem': str(tmp_path / 'local_cache')},
        }
    else:
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
            },
            'local': {'filesystem': str(tmp_path / 'local_cache')},
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
def gcs_storage(request, gcs_credentials, gcs_test_bucket, monkeypatch, tmp_path):
    """Configure nuthatch to use fake-gcs-server."""
    from nuthatch.config import set_test_config_provider

    if is_integration_mode(request):
        bucket = os.environ.get("GCS_TEST_BUCKET")
        if not bucket:
            pytest.skip("GCS_TEST_BUCKET not set for integration test")
        filesystem = f"gs://{bucket}/nuthatch-test-{uuid.uuid4().hex[:8]}"
        config = {
            'root': {
                'filesystem': filesystem,
                'metadata_location': 'filesystem',
                'filesystem_options': {},  # Uses application default credentials
            },
            'local': {'filesystem': str(tmp_path / 'local_cache')},
        }
    else:
        filesystem = f"gs://{gcs_test_bucket}/cache"
        config = {
            'root': {
                'filesystem': filesystem,
                'metadata_location': 'filesystem',
                'filesystem_options': {
                    'token': 'anon',  # For gcsfs/fsspec
                    'service_account_file': gcs_credentials["service_account_file"],  # For delta-rs
                },
            },
            'local': {'filesystem': str(tmp_path / 'local_cache')},
        }
        monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs_credentials["endpoint_url"])

    set_test_config_provider(lambda: config)

    yield {
        "uri": filesystem,
        "bucket": gcs_test_bucket,
        "credentials": gcs_credentials,
        "provider": "gcs",
    }

    set_test_config_provider(None)


@pytest.fixture
def azure_storage(request, azure_credentials, azure_test_container, tmp_path):
    """Configure nuthatch to use Azurite."""
    from nuthatch.config import set_test_config_provider

    if is_integration_mode(request):
        container = os.environ.get("AZURE_TEST_CONTAINER")
        if not container:
            pytest.skip("AZURE_TEST_CONTAINER not set for integration test")
        filesystem = f"abfs://{container}/nuthatch-test-{uuid.uuid4().hex[:8]}"
        config = {
            'root': {
                'filesystem': filesystem,
                'metadata_location': 'filesystem',
                'filesystem_options': {
                    'connection_string': os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
                },
            },
            'local': {'filesystem': str(tmp_path / 'local_cache')},
        }
    else:
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
                    'azure_storage_use_emulator': '1',
                },
            },
            'local': {'filesystem': str(tmp_path / 'local_cache')},
        }

    set_test_config_provider(lambda: config)

    yield {
        "uri": filesystem,
        "container": azure_test_container,
        "credentials": azure_credentials,
        "provider": "azure",
    }

    set_test_config_provider(None)


# =============================================================================
# PostgreSQL Fixtures (for SQL backend testing)
# =============================================================================

@pytest.fixture(scope="session")
def postgres_container():
    """Start a PostgreSQL container for SQL backend testing."""
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:15")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def postgres_credentials(postgres_container):
    """Get PostgreSQL credentials from the test container."""
    return {
        "driver": "postgresql",
        "username": postgres_container.username,
        "password": postgres_container.password,
        "host": postgres_container.get_container_host_ip(),
        "port": postgres_container.get_exposed_port(5432),
        "database": postgres_container.dbname,
    }


@pytest.fixture
def postgres_storage(request, postgres_credentials, tmp_path):
    """
    Configure nuthatch to use PostgreSQL for SQL backend.

    Local mode: uses PostgreSQL container
    Integration mode (-m integration): uses live PostgreSQL credentials from env
    """
    from nuthatch.config import set_test_config_provider

    if is_integration_mode(request):
        host = os.environ.get("POSTGRES_HOST")
        if not host:
            pytest.skip("POSTGRES_HOST not set for integration test")
        credentials = {
            "driver": os.environ.get("POSTGRES_DRIVER", "postgresql"),
            "username": os.environ.get("POSTGRES_USERNAME"),
            "password": os.environ.get("POSTGRES_PASSWORD"),
            "host": host,
            "port": os.environ.get("POSTGRES_PORT", 5432),
            "database": os.environ.get("POSTGRES_DATABASE"),
        }
    else:
        credentials = postgres_credentials

    config = {
        'root': {
            'filesystem': str(tmp_path / 'nuthatch_cache'),
            'metadata_location': 'filesystem',
            'sql': credentials,
        }
    }

    set_test_config_provider(lambda: config)

    yield {"credentials": credentials, "config": config}

    set_test_config_provider(None)
