from nuthatch.backend import get_default_backend

def test_backend_default_registration():
    import pandas as pd

    assert get_default_backend(pd.DataFrame) == 'delta'

def test_backend_other_registration():
    assert get_default_backend(type(5)) == 'basic'

