from cacheable.config import get_config, cacheable_config_parameter
from cacheable.backends import SQLBackend

def test_get_config():
    print(get_config(location='base', backendType=SQLBackend))

def test_config_reg():

    @cacheable_config_parameter('username', location='base')
    def username():
        return 'test_username'

    print(get_config(location='base', backendType=SQLBackend))

def test_config_backend_reg():

    @cacheable_config_parameter('username', location='base', backend='sql')
    def username():
        return 'test_username'

    @cacheable_config_parameter('password', location='base', backend='sql')
    def password():
        return 'test_password'

    print(get_config(location='base', backendType=SQLBackend))



if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
