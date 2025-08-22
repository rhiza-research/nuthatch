from nuthatch.config import get_config
from nuthatch import config_parameter
from nuthatch.backends import SQLBackend
from nuthatch.backends import DeltaBackend

def test_get_config():
    print(get_config(location='root', backend_class=SQLBackend))

def test_config_reg():

    @config_parameter('username', location='root')
    def username():
        return 'test_username'

    print(get_config(location='root', backend_class=SQLBackend))

def test_config_backend_reg():

    @config_parameter('username', location='root', backend='sql')
    def username():
        return 'test_username'

    @config_parameter('password', location='root', backend='sql')
    def password():
        return 'test_password'

    print(get_config(location='root', backend_class=SQLBackend))
    print(get_config(location='root', backend_class=DeltaBackend))



if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
