# Testing

Nuthatch uses pytest for testing with testcontainers for local cloud emulation.

## Quick Start

```bash
# Install dev dependencies
uv sync --group dev --group cloud-test

# Run all tests (requires Docker for testcontainers)
uv run pytest

# Run tests for a specific cloud provider
uv run pytest -m gcs    # GCS tests only (uses fake-gcs-server container)
uv run pytest -m s3     # S3 tests only (uses LocalStack container)
uv run pytest -m azure  # Azure tests only (uses Azurite container)

# Run SQL backend tests (uses postgres container)
uv run pytest tests/backends/test_sql.py

# Run tests without cloud storage backends (still needs Docker for postgres)
uv run pytest -m "not (s3 or gcs or azure)"
```

## Test Structure

```
tests/
├── conftest.py              # Cloud storage fixtures (testcontainers)
├── backends/
│   ├── test_basic.py        # Pickle backend tests
│   ├── test_delta.py        # Delta Lake backend tests
│   ├── test_parquet.py      # Parquet backend tests
│   ├── test_sql.py          # SQL backend tests
│   └── test_zarr.py         # Zarr backend tests
├── processors/
│   └── test_timeseries.py   # Timeseries processor tests
├── test_core.py             # Core caching functionality
├── test_local.py            # Local cache tests
├── test_mirror_storage.py   # Mirror storage tests
└── ...
```

## Test Markers

Tests that need cloud storage are marked with provider markers:

| Marker | Description |
|--------|-------------|
| `@pytest.mark.s3` | Test requires S3 (uses LocalStack container) |
| `@pytest.mark.gcs` | Test requires GCS (uses fake-gcs-server container) |
| `@pytest.mark.azure` | Test requires Azure Blob (uses Azurite container) |

Run specific providers:
```bash
uv run pytest -m gcs           # Only GCS tests
uv run pytest -m "s3 or gcs"   # S3 and GCS tests
uv run pytest -m "not azure"   # Skip Azure tests
```

## Local Container Testing

By default, cloud tests run against local emulators using testcontainers:

| Provider | Container |
|----------|-----------|
| S3 | LocalStack |
| GCS | fake-gcs-server (rhiza fork with XML API support) |
| Azure | Azurite |
| PostgreSQL | postgres:15 |

Docker must be running for container tests.

## Integration Testing (Live Cloud)

To run tests against real cloud services instead of containers, run the test suite directly on the host machine (outside of Docker). The dockerized tests run all emulated containers; running on the host uses your local cloud credentials.

```bash
uv run pytest                    # All cloud tests against live services
uv run pytest -m gcs             # GCS tests only
uv run pytest -m s3              # S3 tests only
uv run pytest -m azure           # Azure tests only
uv run pytest -m "s3 or gcs"     # Multiple providers
```

### Required Environment Variables

**GCS:**
```bash
export GCS_TEST_BUCKET="nuthatch-test"
# Uses application default credentials (gcloud auth application-default login)
```

**S3:**
```bash
export AWS_TEST_BUCKET="nuthatch-test"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."  # Optional, for SSO/assumed roles
```

**Azure:**
```bash
export AZURE_TEST_CONTAINER="nuthatch-test"
export AZURE_STORAGE_CONNECTION_STRING="..."
```

**PostgreSQL:**
```bash
export POSTGRES_HOST="..."
export POSTGRES_USERNAME="..."
export POSTGRES_PASSWORD="..."
export POSTGRES_DATABASE="..."
export POSTGRES_PORT="5432"      # Optional, defaults to 5432
export POSTGRES_DRIVER="postgresql"  # Optional
```

## Writing Cloud Tests

Use the `cloud_storage` fixture and mark with supported providers:

```python
import pytest

@pytest.mark.s3
@pytest.mark.gcs
@pytest.mark.azure
def test_my_feature(cloud_storage):
    """Test runs once per marked provider."""
    # cloud_storage["provider"] is "s3", "gcs", or "azure"
    # cloud_storage["config"] has the nuthatch config

    from nuthatch import cache

    @cache()
    def my_function():
        return {"data": 123}

    result = my_function()
    assert result == {"data": 123}
```

For tests that need explicit config control:

```python
from tests.conftest import test_config

@pytest.mark.gcs
def test_custom_config(cloud_storage):
    config = cloud_storage["config"]
    config["root"]["some_setting"] = "custom_value"

    with cloud_storage.config_context(config):
        # nuthatch uses custom config here
        result = my_cached_function()
```

## Coverage

Coverage reports are generated automatically:
- Terminal: `--cov-report=term-missing`
- HTML: `htmlcov/index.html`
- XML: `coverage.xml` (for CI)

View HTML report:
```bash
open htmlcov/index.html
```

## Test Infrastructure

Terraform configurations for provisioning test resources are in `test_infrastructure/`.

### Setup

```bash
cd test_infrastructure

# Initialize terraform
terraform init

# Review the plan
terraform plan

# Apply (creates resources)
terraform apply
```

### GCS Resources

Creates:
- Storage bucket (`nuthatch-test`)
- Service account with `roles/storage.objectAdmin`
- Service account key file

Outputs:
- `gcs_bucket_name`
- `gcs_key_file`
- `gcs_service_account_email`
- `gcs_project`

### AWS Resources

Creates:
- S3 bucket (`nuthatch-test`)
- IAM user with scoped S3 policy
- Access keys

Outputs:
- `aws_bucket_name`
- `aws_access_key_id`
- `aws_secret_access_key` (sensitive)

### Azure Resources (Currently Disabled)

Creates:
- Resource group (`nuthatch-test-rg`)
- Storage account (`nuthatchtest<random>`)
- Blob container (`nuthatch-test`)

Outputs:
- `azure_container_name`
- `azure_connection_string` (sensitive)
- `azure_resource_group`
- `azure_storage_account`

### Importing Existing Resources

If you already have test resources, import them into terraform state:

**AWS:**
```bash
terraform import aws_s3_bucket.test nuthatch-test
terraform import aws_iam_user.test nuthatch-test
terraform import aws_iam_user_policy.test nuthatch-test:nuthatch-test-s3
```

**GCS:**
```bash
terraform import google_storage_bucket.test nuthatch-test
terraform import google_service_account.test projects/sheerwater/serviceAccounts/nuthatch-test@sheerwater.iam.gserviceaccount.com
```

**Azure:**
```bash
terraform import azurerm_resource_group.test /subscriptions/<subscription-id>/resourceGroups/nuthatch-test-rg
terraform import azurerm_storage_account.test /subscriptions/<subscription-id>/resourceGroups/nuthatch-test-rg/providers/Microsoft.Storage/storageAccounts/<account-name>
terraform import azurerm_storage_container.test https://<account-name>.blob.core.windows.net/nuthatch-test
```

### Configuration

Default values in `*.tf` files:

| Variable | Default | Description |
|----------|---------|-------------|
| `gcs_project` | `sheerwater` | GCP project ID |
| `gcs_bucket_name` | `nuthatch-test` | GCS bucket name |
| `gcs_location` | `us-central1` | GCS bucket location |
| `aws_bucket_name` | `nuthatch-test` | S3 bucket name |
| `aws_region` | `us-east-1` | AWS region |
| `aws_profile` | `rhiza` | AWS credentials profile |
| `azure_container_name` | `nuthatch-test` | Azure blob container name |
| `azure_location` | `eastus` | Azure region |

Override personal credentials with a `terraform.tfvars` file:
```hcl
aws_profile = "my-aws-profile"
```
