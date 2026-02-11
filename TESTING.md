# Testing

Nuthatch uses pytest with Docker Compose for isolated cloud storage testing.

## Quick Start

```bash
# Install dev dependencies
uv sync --group dev

# Run all tests in Docker (recommended, ~40s)
docker compose run --rm test

# Run tests with verbose output, stop on first failure
docker compose run --rm test -xv

# Run tests for a specific cloud provider
docker compose run --rm test -m gcs    # GCS only
docker compose run --rm test -m s3     # S3 only
docker compose run --rm test -m azure  # Azure only

# Run tests locally (slower, uses real cloud credentials from config files)
uv run pytest
```

## Docker Compose Environment

Tests run against local emulators in an isolated Docker network with no internet access:

| Service | Emulator | Purpose |
|---------|----------|---------|
| `aws` | LocalStack | S3 emulation |
| `gcs` | fake-gcs-server | GCS emulation |
| `azurite` | Azurite | Azure Blob Storage emulation |
| `postgres` | PostgreSQL 15 | SQL backend testing |

Init containers (`aws-init`, `gcs-init`, `azurite-init`) pre-create buckets and containers before tests start.

Configuration for each emulator is in `nuthatch.*.docker.toml` files, which are mounted over the regular config files in the test container.

```bash
# Start services and run tests
docker compose run --rm test

# Run with Podman
podman compose run --rm test

# Clean up
docker compose down -v
```

## Test Structure

```
tests/
├── conftest.py                  # Fixtures: cloud_storage, local_config, postgres_storage
├── fixtures/
│   ├── fake_gcs_service_account.json
│   └── nuthatch.toml
├── backends/
│   ├── tabular_test.py          # Shared tabular backend test helpers
│   ├── test_basic.py            # Pickle backend tests
│   ├── test_delta.py            # Delta Lake backend tests
│   ├── test_parquet.py          # Parquet backend tests
│   ├── test_sql.py              # SQL backend tests
│   ├── test_terracotta.py       # Terracotta (PostgreSQL) backend tests
│   └── test_zarr.py             # Zarr backend tests
├── processors/
│   └── test_timeseries.py       # Timeseries processor tests
├── test_backend_identification.py
├── test_cache_args.py           # Cache argument handling
├── test_cli.py                  # CLI command tests
├── test_config_registration.py  # @config_parameter decorator tests
├── test_core.py                 # Core caching functionality
├── test_engines.py              # Engine selection tests
├── test_global_filesystem.py    # Global filesystem config tests
├── test_local.py                # Local cache tests
├── test_memoizer.py             # Memoizer tests
├── test_mirror_storage.py       # Mirror storage tests
├── test_namespace.py            # Namespace tests
├── test_nuthatch_metastore.py   # Metastore tests
└── test_unused_fixture.py       # Validates cloud_storage access tracking
```

## Test Markers

Tests that need cloud storage are marked with provider markers:

| Marker | Description |
|--------|-------------|
| `@pytest.mark.s3` | Test requires S3 (LocalStack in Docker, real S3 locally) |
| `@pytest.mark.gcs` | Test requires GCS (fake-gcs-server in Docker, real GCS locally) |
| `@pytest.mark.azure` | Test requires Azure Blob (Azurite in Docker, real Azure locally) |

Run specific providers:
```bash
docker compose run --rm test -m gcs           # Only GCS tests
docker compose run --rm test -m "s3 or gcs"   # S3 and GCS tests
docker compose run --rm test -m "not azure"   # Skip Azure tests
```

## Test Fixtures

### `cloud_storage` (parametrized)

For tests that interact with cloud storage. Reads per-provider TOML config, creates a unique filesystem path prefix for test isolation, and tracks access to detect unnecessary usage.

```python
@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
def test_my_feature(cloud_storage):
    """Test runs once per marked provider."""
    # cloud_storage["provider"] is "s3", "gcs", or "azure"
    # cloud_storage["config"] has the nuthatch config dict

    @cache()
    def my_function():
        return {"data": 123}

    result = my_function()
    assert result == {"data": 123}
```

Tests that request `cloud_storage` but never access it will fail with an error asking you to use `local_config` instead.

To temporarily modify config during a test:
```python
def test_custom_config(cloud_storage):
    config = cloud_storage["config"]
    modified = {**config, "root": {**config["root"], "some_setting": "custom"}}

    with cloud_storage.config_context(modified):
        # nuthatch reads modified config here
        result = my_cached_function()
```

### `local_config`

For tests that need valid nuthatch config but don't interact with cloud storage (memoizer tests, local caching tests):

```python
def test_local_caching(local_config):
    @cache(cache=True)
    def my_function():
        return [1, 2, 3]

    result = my_function()
    assert result == [1, 2, 3]
```

### `postgres_storage`

For tests that need PostgreSQL:

```python
def test_sql_backend(postgres_storage):
    # postgres_storage["credentials"] has connection details
    # postgres_storage["config"] has the nuthatch config
    ...
```

## Live Cloud Testing

To run tests against real cloud services (outside Docker), configure credentials in TOML config files:

- `nuthatch.aws.toml` — S3 credentials
- `nuthatch.gcp.toml` — GCS credentials
- `nuthatch.azure.toml` — Azure credentials

These files are generated by Terraform (see `test_infrastructure/`) or can be created manually following the `[root]` / `[root.filesystem_options]` / `[local]` format.

```bash
uv run pytest              # All tests against live services
uv run pytest -m gcs       # GCS tests only
uv run pytest -m s3        # S3 tests only
```

## Test Infrastructure

Terraform configurations for provisioning cloud test resources are in `test_infrastructure/`.

```bash
cd test_infrastructure
terraform init
terraform plan
terraform apply
```

This creates test buckets/containers and service accounts for each cloud provider, and outputs `nuthatch.*.toml` config files with credentials.

## CI

Tests run in GitHub Actions via Docker Compose (`.github/workflows/test.yml`):

1. Build test image and cache on GHCR
2. Run `docker compose run --rm test` with coverage
3. Publish JUnit test results and Codecov coverage
4. Archive test artifacts

A separate workflow (`.github/workflows/test-live.yml`) runs tests against real cloud infrastructure on demand.
