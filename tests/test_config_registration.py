from nuthatch.config import NuthatchConfig
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


if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
