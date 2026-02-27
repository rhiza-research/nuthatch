"""Tests for multi-package config resolution.

Verifies that installed packages' nuthatch.toml configs are wrapped as mirrors,
while local packages' configs merge flat. Uses real installed packages
(fake_weather_lib, fake_ocean_lib) in site-packages.

Config-level tests verify NuthatchConfig dict structure directly.
Integration tests call @cache functions from installed packages and verify
caching writes to the project root, not the dependency's configured root.
"""
import pytest
import nuthatch.config
from nuthatch.config import NuthatchConfig, NUTHATCH_PROJECT_CONFIG_ENV, NUTHATCH_GLOBAL_CONFIG_ENV


@pytest.fixture(autouse=True)
def isolate_config(tmp_path, monkeypatch):
    """Isolate every test from real config files and dynamic parameters."""
    nuthatch.config.dynamic_parameters.clear()

    project_config = tmp_path / "nuthatch.toml"
    project_config.write_text('[root]\nfilesystem = "/tmp/project-root"\n')
    global_config = tmp_path / "global.toml"
    global_config.write_text("")

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))
    monkeypatch.delenv("NUTHATCH_ALLOW_INSTALLED_PACKAGE_CONFIGURATION", raising=False)

    yield

    nuthatch.config.dynamic_parameters.clear()


@pytest.fixture
def project_root(tmp_path, monkeypatch):
    """Set up a real local filesystem root for integration tests."""
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
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(temp_config_file))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(fake_home / ".nuthatch.toml"))

    return root_cache


def test_installed_package_config_becomes_mirror():
    """An installed package's root config should appear under mirrors."""
    import fake_weather_lib

    config = NuthatchConfig(wrapped_module=fake_weather_lib)

    # Project root is preserved
    assert config['root']['filesystem'] == "/tmp/project-root"

    # Dependency's root becomes a mirror
    assert 'fake_weather_lib-root' in config['mirrors']
    assert config['mirrors']['fake_weather_lib-root']['filesystem'] == "/tmp/weather-caches"

    # Dependency's own mirrors are prefixed with the package name
    assert 'fake_weather_lib-shared' in config['mirrors']
    assert config['mirrors']['fake_weather_lib-shared']['filesystem'] == "/tmp/weather-shared"


def test_two_installed_packages_independent():
    """Each dependency produces its own mirror entries independently."""
    import fake_weather_lib
    import fake_ocean_lib

    weather_config = NuthatchConfig(wrapped_module=fake_weather_lib)
    ocean_config = NuthatchConfig(wrapped_module=fake_ocean_lib)

    # Weather config has weather mirrors but not ocean mirrors
    assert 'fake_weather_lib-root' in weather_config['mirrors']
    assert 'fake_ocean_lib-root' not in weather_config['mirrors']

    # Ocean config has ocean mirrors but not weather mirrors
    assert 'fake_ocean_lib-root' in ocean_config['mirrors']
    assert 'fake_weather_lib-root' not in ocean_config['mirrors']

    # Both preserve the project root
    assert weather_config['root']['filesystem'] == "/tmp/project-root"
    assert ocean_config['root']['filesystem'] == "/tmp/project-root"

    # Each mirror has the correct filesystem
    assert weather_config['mirrors']['fake_weather_lib-root']['filesystem'] == "/tmp/weather-caches"
    assert ocean_config['mirrors']['fake_ocean_lib-root']['filesystem'] == "/tmp/ocean-caches"


def test_local_module_merges_flat():
    """A module from the local project (not site-packages) merges flat."""
    import nuthatch.config as local_module

    # nuthatch itself is NOT in site-packages (it's the project under test)
    assert 'site-packages' not in local_module.__file__

    config = NuthatchConfig(wrapped_module=local_module)

    # Config merges flat — no mirrors created from the local module's config
    # (the local module doesn't have its own nuthatch.toml in its package dir,
    # so wrapped_config is empty, but the key point is the code path:
    # it goes through the flat merge branch, not the mirror-wrapping branch)
    assert config['root']['filesystem'] == "/tmp/project-root"


def test_allow_installed_package_configuration(monkeypatch):
    """NUTHATCH_ALLOW_INSTALLED_PACKAGE_CONFIGURATION makes installed packages merge flat."""
    import fake_weather_lib

    monkeypatch.setenv("NUTHATCH_ALLOW_INSTALLED_PACKAGE_CONFIGURATION", "true")
    config = NuthatchConfig(wrapped_module=fake_weather_lib)

    # With the override, the installed package's config merges flat
    # Its root overwrites the project root
    assert config['root']['filesystem'] == "/tmp/weather-caches"

    # No mirrors created — everything merged flat
    assert 'fake_weather_lib-root' not in config.get('mirrors', {})


# =============================================================================
# Integration tests — call @cache functions from installed dependencies
# and verify the config that was actually used during the call.
# =============================================================================


@pytest.fixture
def capture_config(monkeypatch):
    """Capture top-level NuthatchConfig instances created during @cache calls.

    Filters out sub-configs (created with sub_config= for individual locations).
    """
    configs = []
    original_init = NuthatchConfig.__init__

    def capturing_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        # Only capture top-level configs (sub_config=None means full merge was done)
        if kwargs.get('sub_config') is None and (len(args) < 3 or args[2] is None):
            configs.append(self)

    monkeypatch.setattr(NuthatchConfig, '__init__', capturing_init)
    return configs


def test_two_dependencies_resolve_independent_configs(project_root, capture_config):
    """Two installed dependencies each get independent configs with their own mirrors."""
    from fake_weather_lib.forecasts import fake_weather_cached_fn
    from fake_ocean_lib.sst import fake_ocean_cached_fn

    fake_weather_cached_fn(key='multi')
    fake_ocean_cached_fn(key='multi')

    # Two configs were created (one per @cache call)
    assert len(capture_config) == 2
    weather_config, ocean_config = capture_config

    # Both use the project root for writing
    assert weather_config['root']['filesystem'] == str(project_root)
    assert ocean_config['root']['filesystem'] == str(project_root)

    # Each has only its own dependency as a mirror
    assert 'fake_weather_lib-root' in weather_config['mirrors']
    assert 'fake_ocean_lib-root' not in weather_config['mirrors']

    assert 'fake_ocean_lib-root' in ocean_config['mirrors']
    assert 'fake_weather_lib-root' not in ocean_config['mirrors']

    # Both cached to project root
    pkl_files = list(project_root.rglob('*.pkl'))
    assert len(pkl_files) >= 2
