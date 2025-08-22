from nuthatch.backend import get_default_backend
from nuthatch.backend import NuthatchBackend
from nuthatch.backend import register_backend

@register_backend
class TestBackend(NuthatchBackend):
    import pandas as pd

    backend_name = 'test'
    default_for_type = pd.DataFrame

def test_backend_default_registration():
    import pandas as pd

    assert get_default_backend(pd.DataFrame) == 'test'

def test_backend_other_registration():
    assert get_default_backend(type(5)) == 'basic'

