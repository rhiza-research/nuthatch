from nuthatch.config import get_config
from nuthatch import config_parameter
from nuthatch.backends import SQLBackend
from nuthatch.backends import DeltaBackend

def test_get_config():
    get_config(location='root', requested_parameters = SQLBackend.config_parameters, backend_name=SQLBackend.backend_name)

def test_config_reg():

    @config_parameter('username', location='root')
    def username():
        return 'test_username'

    sql = get_config(location='root', requested_parameters = SQLBackend.config_parameters, backend_name=SQLBackend.backend_name)
    assert sql['username'] == 'test_username'

def test_config_backend_reg():

    @config_parameter('username', location='root', backend='sql')
    def username():
        return 'test_username'

    @config_parameter('password', location='root', backend='sql')
    def password():
        return 'test_password'

    sql = get_config(location='root', requested_parameters = SQLBackend.config_parameters, backend_name=SQLBackend.backend_name)
    assert sql['username'] == 'test_username'
    assert sql['password'] == 'test_password'


if __name__ == '__main__':
    test_get_config()
    test_config_reg()
    test_config_backend_reg()
