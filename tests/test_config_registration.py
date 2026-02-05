import os
import warnings
import pytest
from nuthatch.config import NuthatchConfig, NUTHATCH_GLOBAL_CONFIG_ENV, NUTHATCH_PROJECT_CONFIG_ENV, STRICT_MODE_ENV
from nuthatch import config_parameter


def test_get_config():
    config = NuthatchConfig(wrapped_module='tests')
    assert config

def test_config_reg():

    @config_parameter('username2', location='root')
    def username():
        return 'test_username'

    config = NuthatchConfig(wrapped_module='tests')
    assert config['root']['username2'] == 'test_username'

def test_config_backend_reg():

    @config_parameter('username2', location='root', backend='sql')
    def username():
        return 'test_username'

    @config_parameter('password2', location='root', backend='sql')
    def password():
        return 'test_password'

    config = NuthatchConfig(wrapped_module='tests')
    assert config['root']['sql']['username2'] == 'test_username'
    assert config['root']['sql']['password2'] == 'test_password'


def test_global_config_env_var(tmp_path, monkeypatch):
    """Test NUTHATCH_GLOBAL_CONFIG env var overrides default location."""
    config_file = tmp_path / "custom-global.toml"
    config_file.write_text("[filesystem_options]\ncache_timeout = 123\n")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(config_file))
    # Also set project config to empty to isolate the test
    project_config = tmp_path / "project.toml"
    project_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))

    # Use 'isolated_test' as module name to avoid dynamic configs registered by tests/__init__.py
    config = NuthatchConfig(wrapped_module='isolated_test')
    # filesystem_options is a global setting, accessed at top level (not per-location)
    assert config['filesystem_options']['cache_timeout'] == 123


def test_project_config_env_var(tmp_path, monkeypatch):
    """Test NUTHATCH_PROJECT_CONFIG env var overrides default location."""
    config_file = tmp_path / "custom-project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/test-project"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    # Set global config to empty
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    # Use 'isolated_test' as module name to avoid dynamic configs registered by tests/__init__.py
    config = NuthatchConfig(wrapped_module='isolated_test')
    assert config['root']['filesystem'] == "/tmp/test-project"


def test_global_config_rejects_invalid_keys(tmp_path, monkeypatch):
    """Test that global config validation rejects invalid keys."""
    # Set env vars to isolate test
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(tmp_path / "project.toml"))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(tmp_path / "global.toml"))

    config = NuthatchConfig(wrapped_module='isolated_test')
    invalid_config = {'filesystem': '/tmp/test', 'other_key': 'value'}
    with pytest.raises(ValueError, match="is invalid"):
        config._validate_global_config(invalid_config)


def test_global_config_accepts_filesystem_options(tmp_path, monkeypatch):
    """Test that global config validation accepts filesystem_options."""
    # Set env vars to isolate test
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(tmp_path / "project.toml"))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(tmp_path / "global.toml"))

    config = NuthatchConfig(wrapped_module='isolated_test')
    valid_config = {'filesystem_options': {'token': 'anon', 'cache_timeout': 0}}
    result = config._validate_global_config(valid_config)
    assert result == valid_config


def test_global_config_accepts_skipped_filesystems(tmp_path, monkeypatch):
    """Test that global config validation accepts skipped_filesystems."""
    # Set env vars to isolate test
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(tmp_path / "project.toml"))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(tmp_path / "global.toml"))

    config = NuthatchConfig(wrapped_module='isolated_test')
    valid_config = {'skipped_filesystems': ['gs://bucket1', 's3://bucket2']}
    result = config._validate_global_config(valid_config)
    assert result == valid_config


def test_pyproject_toml_ignored(tmp_path, monkeypatch):
    """Test that pyproject.toml is no longer read for nuthatch config."""
    # Create pyproject.toml with nuthatch config
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.nuthatch]\nfilesystem = "/from/pyproject"\n')

    # Create nuthatch.toml with different config (using new explicit [root] format)
    nuthatch_toml = tmp_path / "nuthatch.toml"
    nuthatch_toml.write_text('[root]\nfilesystem = "/from/nuthatch"\n')

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(nuthatch_toml))
    # Set global config to empty
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    # Use 'isolated_test' as module name to avoid dynamic configs registered by tests/__init__.py
    config = NuthatchConfig(wrapped_module='isolated_test')
    assert config['root']['filesystem'] == "/from/nuthatch"


def test_implicit_root_deprecation_warning(tmp_path, monkeypatch):
    """Test that top-level config keys emit deprecation warning."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('filesystem = "/tmp/implicit-root"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = NuthatchConfig(wrapped_module='isolated_test')
        # Should still work (backward compat)
        assert config['root']['filesystem'] == "/tmp/implicit-root"
        # Should emit deprecation warning
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1
        assert "Top-level config key" in str(deprecation_warnings[0].message)
        assert "[root] section" in str(deprecation_warnings[0].message)


def test_mirror_prefix_deprecation_warning(tmp_path, monkeypatch):
    """Test that [mirror-*] sections emit deprecation warning."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/root"\n\n[mirror-public]\nfilesystem = "/tmp/public"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = NuthatchConfig(wrapped_module='isolated_test')
        # Should still work (backward compat via flattening)
        assert config['mirror-public']['filesystem'] == "/tmp/public"
        # Should emit deprecation warning
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1
        assert "[mirror-public]" in str(deprecation_warnings[0].message)
        assert "[mirrors.public]" in str(deprecation_warnings[0].message)


def test_new_mirrors_format_no_warning(tmp_path, monkeypatch):
    """Test that new [mirrors.*] format does not emit deprecation warning."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/root"\n\n[mirrors.public]\nfilesystem = "/tmp/public"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        config = NuthatchConfig(wrapped_module='isolated_test')
        # Should work with new format
        assert config['mirror-public']['filesystem'] == "/tmp/public"
        # Also available via mirrors dict
        assert config['mirrors']['public']['filesystem'] == "/tmp/public"
        # Should NOT emit deprecation warning
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0


def test_strict_mode_raises_error(tmp_path, monkeypatch):
    """Test that strict mode raises error on deprecated format."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('filesystem = "/tmp/implicit-root"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))
    monkeypatch.setenv(STRICT_MODE_ENV, "true")

    with pytest.raises(ValueError, match="Top-level config key"):
        NuthatchConfig(wrapped_module='isolated_test')


def test_strict_mode_mirror_prefix_raises_error(tmp_path, monkeypatch):
    """Test that strict mode raises error on deprecated mirror format."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/root"\n\n[mirror-public]\nfilesystem = "/tmp/public"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))
    monkeypatch.setenv(STRICT_MODE_ENV, "true")

    with pytest.raises(ValueError, match="mirror.*deprecated"):
        NuthatchConfig(wrapped_module='isolated_test')


def test_env_var_mirrors_format(tmp_path, monkeypatch):
    """Test new NUTHATCH_MIRRORS_<name>_<param> env var format."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/root"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))
    monkeypatch.setenv("NUTHATCH_MIRRORS_PUBLIC_FILESYSTEM", "/tmp/env-public")

    config = NuthatchConfig(wrapped_module='isolated_test')
    assert config['mirror-public']['filesystem'] == "/tmp/env-public"


if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
