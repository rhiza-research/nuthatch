import pytest
from ..backends.tabular_test import multi_tab_test_pandas, multi_tab_test_dask
from nuthatch.config import set_test_config_provider

# All tests in this module use postgres, not cloud storage
pytestmark = pytest.mark.no_cloud

# Check if testcontainers is available
try:
    from testcontainers.postgres import PostgresContainer
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


@pytest.fixture(scope="module")
def postgres_container():
    """Start a PostgreSQL container for SQL backend testing."""
    if not POSTGRES_AVAILABLE:
        pytest.skip("testcontainers not installed")

    container = PostgresContainer("postgres:15")
    container.start()

    yield container

    container.stop()


@pytest.fixture(autouse=True)
def postgres_config(postgres_container, tmp_path):
    """Configure nuthatch to use the PostgreSQL test container."""
    # Get connection details from container
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)

    config = {
        'root': {
            # Filesystem for metastore
            'filesystem': str(tmp_path / 'nuthatch_cache'),
            'metadata_location': 'filesystem',
            # SQL backend config
            'sql': {
                'driver': 'postgresql',
                'username': postgres_container.username,
                'password': postgres_container.password,
                'host': host,
                'port': port,
                'database': postgres_container.dbname,
            }
        }
    }

    set_test_config_provider(lambda: config)
    yield
    set_test_config_provider(None)


def test_sql():
    multi_tab_test_pandas(backend='sql')
    multi_tab_test_dask(backend='sql')
