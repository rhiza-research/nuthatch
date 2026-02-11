import pytest
from nuthatch.config import NuthatchConfig, NUTHATCH_GLOBAL_CONFIG_ENV, NUTHATCH_PROJECT_CONFIG_ENV
from nuthatch import config_parameter


def test_get_config(tmp_path, monkeypatch):
    # Isolate from any project root nuthatch.toml
    project_config = tmp_path / "project.toml"
    project_config.write_text('[root]\nfilesystem = "/tmp/test"\n')
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    config = NuthatchConfig(wrapped_module='tests')
    assert config


def test_config_reg(tmp_path, monkeypatch):
    # Isolate from any project root nuthatch.toml
    project_config = tmp_path / "project.toml"
    project_config.write_text('[root]\nfilesystem = "/tmp/test"\n')
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    @config_parameter('username2', location='root')
    def username():
        return 'test_username'

    config = NuthatchConfig(wrapped_module='tests')
    assert config['root']['username2'] == 'test_username'


def test_config_backend_reg(tmp_path, monkeypatch):
    # Isolate from any project root nuthatch.toml
    project_config = tmp_path / "project.toml"
    project_config.write_text('[root]\nfilesystem = "/tmp/test"\n')
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

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
    project_config = tmp_path / "project.toml"
    project_config.write_text("")
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    config = NuthatchConfig(wrapped_module='isolated_test')
    invalid_config = {'filesystem': '/tmp/test', 'other_key': 'value'}
    with pytest.raises(ValueError, match="is invalid"):
        config._validate_global_config(invalid_config)


def test_global_config_accepts_filesystem_options(tmp_path, monkeypatch):
    """Test that global config validation accepts filesystem_options."""
    # Set env vars to isolate test
    project_config = tmp_path / "project.toml"
    project_config.write_text("")
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    config = NuthatchConfig(wrapped_module='isolated_test')
    valid_config = {'filesystem_options': {'token': 'anon', 'cache_timeout': 0}}
    result = config._validate_global_config(valid_config)
    assert result == valid_config


def test_global_config_accepts_skipped_filesystems(tmp_path, monkeypatch):
    """Test that global config validation accepts skipped_filesystems."""
    # Set env vars to isolate test
    project_config = tmp_path / "project.toml"
    project_config.write_text("")
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(project_config))
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    config = NuthatchConfig(wrapped_module='isolated_test')
    valid_config = {'skipped_filesystems': ['gs://bucket1', 's3://bucket2']}
    result = config._validate_global_config(valid_config)
    assert result == valid_config


def test_pyproject_toml_ignored(tmp_path, monkeypatch):
    """Test that pyproject.toml is no longer read for nuthatch config."""
    # Create pyproject.toml with nuthatch config
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.nuthatch]\nfilesystem = "/from/pyproject"\n')

    # Create nuthatch.toml with different config (using explicit [root] format)
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


def test_invalid_top_level_key_raises_error(tmp_path, monkeypatch):
    """Test that invalid top-level config keys raise an error."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('filesystem = "/tmp/implicit-root"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    with pytest.raises(ValueError, match="Invalid top-level config key"):
        NuthatchConfig(wrapped_module='isolated_test')


def test_mirrors_format(tmp_path, monkeypatch):
    """Test [mirrors.*] format works correctly."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/root"\n\n[mirrors.public]\nfilesystem = "/tmp/public"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    config = NuthatchConfig(wrapped_module='isolated_test')
    # Mirrors available via mirrors dict
    assert config['mirrors']['public']['filesystem'] == "/tmp/public"


def test_env_var_mirrors_format(tmp_path, monkeypatch):
    """Test NUTHATCH_MIRRORS_<name>_<param> env var format."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "/tmp/root"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))
    monkeypatch.setenv("NUTHATCH_MIRRORS_PUBLIC_FILESYSTEM", "/tmp/env-public")

    config = NuthatchConfig(wrapped_module='isolated_test')
    assert config['mirrors']['public']['filesystem'] == "/tmp/env-public"


def test_env_var_nested_filesystem_options(tmp_path, monkeypatch):
    """Test NUTHATCH_ROOT_FILESYSTEM_OPTIONS_<key> env var format for nested dicts."""
    config_file = tmp_path / "project.toml"
    config_file.write_text('[root]\nfilesystem = "s3://bucket/cache"\n')
    monkeypatch.setenv(NUTHATCH_PROJECT_CONFIG_ENV, str(config_file))
    global_config = tmp_path / "global.toml"
    global_config.write_text("")
    monkeypatch.setenv(NUTHATCH_GLOBAL_CONFIG_ENV, str(global_config))

    # Set nested filesystem_options via env vars
    monkeypatch.setenv("NUTHATCH_ROOT_FILESYSTEM_OPTIONS_KEY", "my-access-key")
    monkeypatch.setenv("NUTHATCH_ROOT_FILESYSTEM_OPTIONS_SECRET", "my-secret-key")
    monkeypatch.setenv("NUTHATCH_ROOT_FILESYSTEM_OPTIONS_ENDPOINT_URL", "http://localhost:4566")

    config = NuthatchConfig(wrapped_module='isolated_test')
    assert config['root']['filesystem_options']['key'] == "my-access-key"
    assert config['root']['filesystem_options']['secret'] == "my-secret-key"
    assert config['root']['filesystem_options']['endpoint_url'] == "http://localhost:4566"


if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
