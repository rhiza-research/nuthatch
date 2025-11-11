from nuthatch import cache
import pandas as pd
import dask.dataframe as dd

import random
import string

def generate_random_string(length):
    """Generates a random string of specified length using lowercase letters and digits."""
    characters = string.ascii_lowercase + string.digits
    random_string = ''.join(random.choice(characters) for i in range(length))
    return random_string

@cache(cache_args=['name'])
def pandas_df(name='bob'):
    """Test function for tabular data."""
    data = [[name, 10], [generate_random_string(4), 15], [generate_random_string(4), 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    return df

@cache(cache_args=['name'])
def dask_df(name='bob'):
    """Test function for tabular data."""
    data = [[name, 10], [generate_random_string(4), 15], [generate_random_string(4), 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    df = dd.from_pandas(df)
    return df


def multi_tab_test_pandas(backend):
    """Test the tabular function."""
    data = pandas_df('josh', backend=backend, recompute=True, force_overwrite=True)
    data2 = pandas_df('josh', backend=backend, engine='pandas')
    pd.testing.assert_frame_equal(data,data2)

    data3 = pandas_df('josh', recompute=True, backend=backend, force_overwrite=True)
    data4 = pandas_df('josh', backend=backend, engine='pandas')
    try:
        pd.testing.assert_frame_equal(data, data3)
    except AssertionError:
        pass
    else:
        assert False

    pd.testing.assert_frame_equal(data3, data4)

def multi_tab_test_dask(backend):
    """Test the tabular function."""
    data = dask_df('josh', backend=backend, recompute=True, force_overwrite=True)
    data2 = dask_df('josh', backend=backend, engine='dask')
    pd.testing.assert_frame_equal(data.compute(),data2.compute())

    data3 = dask_df('josh', recompute=True, backend=backend, force_overwrite=True)
    data4 = dask_df('josh', backend=backend, engine='dask')
    try:
        pd.testing.assert_frame_equal(data.compute(), data3.compute())
    except AssertionError:
        pass
    else:
        assert False

    pd.testing.assert_frame_equal(data3.compute(), data4.compute())
