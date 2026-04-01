# Nuthatch Configuration Guide

## Overview

Nuthatch uses a layered configuration system. Configuration is loaded from
multiple sources and merged together in a specific order. The final merged
config tells nuthatch where to read/write cached data and how to connect
to storage backends.

## Config Sources

When a `@cache` decorated function is called, nuthatch constructs a config
from these sources:

1. **Global config** (`~/.nuthatch.toml`) — machine-wide settings
2. **Project config** (`nuthatch.toml` found from cwd, or `NUTHATCH_PROJECT_CONFIG`) — your project's cache locations
3. **Environment variables** (`NUTHATCH_ROOT_*`, `NUTHATCH_LOCAL_*`, etc.) — overrides for CI/deployment
4. **Wrapped module config** (`nuthatch.toml` in the decorated function's package directory)
5. **Dynamic config** (`@config_parameter` / `set_parameter`) — runtime config from Python code

How these merge depends on where the `@cache` function is defined:

- **In your project** (not in site-packages): all sources merge flat,
  later sources override earlier ones (1 | 2 | 3 | 4 | 5).
- **In an installed package** (in site-packages): sources 1-3 set your
  project's config. Sources 4-5 from the dependency are added under
  `mirrors` (e.g. `mirrors.{package}-root`) so the dependency's caches
  are read-only and can't overwrite your root.

## Config File Format

### Project config (`nuthatch.toml`)

```toml
# Primary cache location — where data is written
[root]
filesystem = "gs://my-bucket/caches"

[root.filesystem_options]
token = "/path/to/credentials.json"

# Optional local cache — faster reads from local disk
[local]
filesystem = "~/.nuthatch/caches"

# Optional read-only mirrors
[mirrors.public-data]
filesystem = "gs://public-bucket/shared-caches"

# Optional: default cache mode for all functions
cache_mode = "write"
```

### Global config (`~/.nuthatch.toml`)

Global config is intentionally restricted. It can **only** contain:

```toml
# Filesystems to skip (e.g., unreachable buckets)
skipped_filesystems = ["gs://broken-bucket/path"]
```

All other configuration belongs in project-level `nuthatch.toml` files.

## Config Structure

Each **location** (root, local, mirrors) can have:

| Key                  | Description                                          |
|----------------------|------------------------------------------------------|
| `filesystem`         | URI for file backends (`gs://`, `s3://`, `~/local/`)  |
| `filesystem_options` | Dict of [fsspec](https://filesystem-spec.readthedocs.io/) options (credentials, cache settings) |
| `driver`             | SQLAlchemy driver for database backends              |
| `host`, `port`       | Database connection                                  |
| `database`           | Database name                                        |
| `username`, `password` | Database credentials                               |
| `local_cache_size`   | Max in-memory cache for local objects (e.g. `"2GB"`) |
| `remote_cache_size`  | Max in-memory cache for dask/xarray (e.g. `"32GB"`)  |

Locations can also have **backend-specific subsections** that override
the location-level settings for a particular backend:

```toml
[root]
filesystem = "gs://my-bucket/caches"

# Terracotta needs both a filesystem AND a database
[root.terracotta]
filesystem = "gs://my-bucket/rasters"
driver = "postgresql"
host = "localhost"
database = "terracotta_db"
username = "tc_user"
password = "tc_pass"
```

## Environment Variables

Environment variables override config file values. Format:

```
NUTHATCH_ROOT_FILESYSTEM=gs://my-bucket/caches
NUTHATCH_ROOT_FILESYSTEM_OPTIONS_TOKEN=/path/to/creds.json
NUTHATCH_LOCAL_FILESYSTEM=~/.nuthatch/caches
NUTHATCH_MIRRORS_PUBLIC_FILESYSTEM=gs://public-bucket/caches
NUTHATCH_ROOT_TERRACOTTA_HOST=localhost
```

Pattern: `NUTHATCH_{LOCATION}_{PARAM}` or `NUTHATCH_{LOCATION}_{BACKEND}_{PARAM}`

## Special Environment Variables

| Variable                  | Description                                              |
|---------------------------|----------------------------------------------------------|
| `NUTHATCH_PROJECT_CONFIG` | Explicit path to project config file. Skips directory search. |
| `NUTHATCH_GLOBAL_CONFIG`  | Override global config path (default: `~/.nuthatch.toml`)  |
| `NUTHATCH_ALLOW_INSTALLED_PACKAGE_CONFIGURATION` | When `true`, allows an installed package's dynamic parameters to set root config instead of being wrapped as mirrors. Useful on clusters where your package is installed for execution but is the primary project. |

## How Config Works for Your Own Project

When you call a `@cache` decorated function defined in your own project,
nuthatch finds your `nuthatch.toml` by searching upward from cwd (or using
`NUTHATCH_PROJECT_CONFIG` if set).

### Example: Simple project

```
my-project/
├── nuthatch.toml          # Found by searching up from cwd
├── src/
│   └── my_project/
│       └── pipeline.py    # Contains @cache decorated functions
```

```toml
# my-project/nuthatch.toml
[root]
filesystem = "gs://my-project-bucket/caches"

[root.filesystem_options]
token = "/path/to/gcp-credentials.json"
```

```python
# src/my_project/pipeline.py
from nuthatch import cache

@cache(storage_backend="zarr")
def compute_temperature(date):
    # expensive computation...
    return dataset
```

When `compute_temperature()` is called:
1. nuthatch finds `nuthatch.toml` via cwd search (or `NUTHATCH_PROJECT_CONFIG`)
2. Data is cached to `gs://my-project-bucket/caches/my_project/compute_temperature/...`

## How Config Works for Installed Dependencies

When your project depends on another package that also uses nuthatch, the
dependency's config is automatically discovered and merged as **mirrors**.

The key behavior:
- Nuthatch checks the dependency's **package directory only** for a `nuthatch.toml`
  (no upward directory walk from site-packages)
- The dependency's `[root]` becomes a mirror named `{package}-root`
- The dependency's mirrors become `{package}-{mirror_name}`

This means your project can read from the dependency's caches without the
dependency being able to overwrite your project's root cache.

### Example: Two dependencies using nuthatch

```
my-app/
├── nuthatch.toml
├── src/
│   └── my_app/
│       └── main.py           # imports weather_lib and ocean_lib
│
└── .venv/lib/site-packages/
    ├── weather_lib/
    │   ├── __init__.py
    │   ├── nuthatch.toml      # weather_lib's bundled config
    │   └── forecasts.py       # @cache decorated functions
    └── ocean_lib/
        ├── __init__.py
        ├── nuthatch.toml      # ocean_lib's bundled config
        └── sst.py             # @cache decorated functions
```

```toml
# my-app/nuthatch.toml — your project config
[root]
filesystem = "gs://my-app-bucket/caches"
```

```toml
# weather_lib/nuthatch.toml — bundled with the package
[root]
filesystem = "gs://weather-data/caches"
```

```toml
# ocean_lib/nuthatch.toml — bundled with the package
[root]
filesystem = "gs://ocean-data/caches"
```

```python
# my_app/main.py
from weather_lib.forecasts import get_forecast   # @cache decorated
from ocean_lib.sst import get_sst                # @cache decorated

# When get_forecast() is called:
#   Config merges to:
#     root:                          gs://my-app-bucket/caches   (your project)
#     mirrors:
#       weather_lib-root:            gs://weather-data/caches    (from dependency)
#
#   The function reads from the weather_lib-root mirror,
#   writes to your root.

forecast = get_forecast("2026-01-01")

# When get_sst() is called:
#   Config merges to:
#     root:                          gs://my-app-bucket/caches   (your project)
#     mirrors:
#       ocean_lib-root:              gs://ocean-data/caches      (from dependency)

sst = get_sst("2026-01-01")
```

Each dependency's config is loaded independently when its `@cache` functions
are called. The dependency's root becomes a read-only mirror so your project
can access the cached data without risk of overwriting the dependency's
cache locations.

### Packaging a nuthatch.toml with your library

For the config to be found when your package is installed, the `nuthatch.toml`
must be in the package directory (next to `__init__.py`):

```
src/
└── my_library/
    ├── __init__.py
    └── nuthatch.toml
```

Configure your build tool to include it:

**hatch (hatchling):**
```toml
# pyproject.toml
[tool.hatch.build.targets.wheel]
packages = ["src/my_library"]
# nuthatch.toml is included automatically if it's in the package dir
```

**setuptools:**
```toml
# pyproject.toml
[tool.setuptools.package-data]
my_library = ["nuthatch.toml"]
```

During development, placing `nuthatch.toml` at the project root works fine
(nuthatch searches upward from cwd). But for distribution, it must be inside
the package directory.

## Dynamic Configuration

You should not save secrets in config files. Nuthatch provides two ways to
set config at runtime.

### `@config_parameter` decorator

Register a function that returns a config value. The function is called
lazily when the config key is first accessed. **The module containing the
decorator must be imported before any cached functions are called.**

```python
# src/my_project/config.py
from nuthatch.config import config_parameter

@config_parameter('filesystem_options', secret=True)
def fetch_key():
    # Fetch from secret store, environment, etc
    return {
        'key': os.environ['S3_KEY'],
        'secret': os.environ['S3_SECRET']
    }

# Or target a specific backend:
@config_parameter("token", location="root", backend="zarr", secret=True)
def get_gcs_token():
    return fetch_secret("gcs-service-account-key")
```

The `secret=True` flag masks the value when configs are printed (e.g. via CLI).

### `set_parameter`

Set config values directly in code. This is sometimes necessary for
distributed environments where config must be set on workers:

```python
from nuthatch.config import set_parameter
set_parameter({'filesystem': "gs://my-datalake"}, location="root")
```

Dynamic parameters are scoped to the module that registers them and are
merged after all file-based config sources.

## Testing

For tests, use `NUTHATCH_PROJECT_CONFIG` to point to a test-specific config:

```python
# conftest.py
import nuthatch.config

@pytest.fixture
def test_config(tmp_path, monkeypatch):
    config_file = tmp_path / "nuthatch.toml"
    config_file.write_text("""
[root]
filesystem = "{tmp_path}/test-cache"
""")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv(
        nuthatch.config.NUTHATCH_PROJECT_CONFIG_ENV,
        str(config_file)
    )
    monkeypatch.setenv(
        nuthatch.config.NUTHATCH_GLOBAL_CONFIG_ENV,
        str(tmp_path / ".nuthatch.toml")
    )
```

When `NUTHATCH_PROJECT_CONFIG` is set, nuthatch uses that file directly
instead of searching the directory tree. This prevents tests from
accidentally discovering a real `nuthatch.toml` on disk.

Wrapped module configs (from dependencies) are still discovered by checking
the module's own directory. Since test dependencies typically don't bundle
`nuthatch.toml` files, this is safe.
