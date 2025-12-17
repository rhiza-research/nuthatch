import pytest
from nuthatch.config import NuthatchConfig, set_test_config_provider
from nuthatch import config_parameter


@pytest.fixture(autouse=True)
def reset_test_config_provider():
    """Ensure these tests use real config loading, not injected test config.

    Shouldn't be needed since cloud_storage fixture cleans up after itself,
    but ensures an error in a previous test isn't leaving the config provider dirty.
    """
    set_test_config_provider(None)
    yield
    set_test_config_provider(None)


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


if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
