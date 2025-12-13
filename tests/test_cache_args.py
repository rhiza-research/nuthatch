import numpy as np
from nuthatch import cache
from nuthatch import config_parameter

@config_parameter('filesystem', location='root')
def set_filesystem():
    return './.cache'

@config_parameter('filesystem', location='local')
def set_filesystem2():
    return './.cache2'

def test_far_nesting():

    @cache()
    def f1():
        return np.random.randint(100000)

    def f2():
        return f1()

    def f3():
        return f2()

    def f4():
        return f3()

    def f5():
        return f4()

    def f6():
        return f5()

    @cache()
    def f7():
        return f6()

    # Put f1 in the local cache
    num1 = f1(cache_mode='local')

    # They should be the same because f1 should persist
    num2 = f7(cache_mode='local_overwrite' , recompute=True)
    assert num1 == num2

def test_cache_disable_if():
    """Test cache_disable_if argument."""
    @cache(cache_args=['agg_days'],
           cache_disable_if={'agg_days': 7})
    def cached_func(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    @cache(cache_args=['agg_days'],
           cache_disable_if={'agg_days': [1, 7, 14]})
    def cached_func2(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    @cache(cache_args=['agg_days', 'grid'],
           cache_disable_if=[{'agg_days': [1, 7, 14],
                              'grid': 'global1_5'},
                             {'agg_days': 8}])
    def cached_func3(agg_days=7, grid='global0_25'):  # noqa: ARG001
        return np.random.randint(1000)

    # Instantiate the cache
    ds = cached_func(agg_days=1)
    #  Cache should be enabled - these should be equal
    dsp = cached_func(agg_days=1)
    assert ds == dsp

    # Instantiate the cache
    ds = cached_func(agg_days=7)
    #  Cache should be disabled - these should be different random numbers
    dsp = cached_func(agg_days=7)
    assert ds != dsp

    # Should be disabled
    ds = cached_func2(agg_days=14)
    dsp = cached_func2(agg_days=14)
    assert ds != dsp

    # Should be disabled
    ds = cached_func3(agg_days=14, grid='global1_5')
    dsp = cached_func3(agg_days=14, grid='global1_5')
    assert ds != dsp

    # Should be enabled
    ds = cached_func3(agg_days=14, grid='global0_25')
    dsp = cached_func3(agg_days=14, grid='global0_25')
    assert ds == dsp

    # Should be disabled
    ds = cached_func3(agg_days=8)
    dsp = cached_func3(agg_days=8)
    assert ds != dsp

    # Should be enabled
    ds = cached_func3(agg_days=14)
    dsp = cached_func3(agg_days=14)
    assert ds == dsp


def test_cache_arg_scope():
    """Test that argument scope is not improperly global/inherited between calls."""
    @cache(cache_args=['agg_days'],
           cache_disable_if={'agg_days': 7})
    def cached_func(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    # Instantiate the cache
    ds = cached_func(agg_days=1)
    #  Cache should be enabled - these should be equal
    dsp = cached_func(agg_days=1)
    assert ds == dsp

    # Instantiate the cache
    ds = cached_func(agg_days=7)
    #  Cache should be disabled - these should be different random numbers
    dsp = cached_func(agg_days=7)
    assert ds != dsp

    # Retest with agg days 1
    ds = cached_func(agg_days=1)
    #  Cache should be disabled - these should be different random numbers
    dsp = cached_func(agg_days=1)
    assert ds == dsp

def test_deep_cache():
    """Test the deep cache recompute functionality."""

    @cache(cache_args=[])
    def deep_cached_func():  # noqa: ARG001
        return np.random.randint(1000000)

    @cache(cache_args=[])
    def deep_cached_func2():  # noqa: ARG001
        return deep_cached_func() + np.random.randint(1000000)

    @cache(cache_args=[])
    def deep_cached_func3():  # noqa: ARG001
        return deep_cached_func2()

    first = deep_cached_func3()
    second = deep_cached_func3()
    assert first == second

    # now verify the recomputing all the way back works
    fourth = deep_cached_func3(recompute=["deep_cached_func", "deep_cached_func2"], cache_mode='overwrite')
    assert first != fourth

    # now verify that just recompute the second one works
    init = deep_cached_func()
    second = fourth - init
    final = deep_cached_func3(recompute="deep_cached_func2", cache_mode='overwrite')
    dsecond = final - init
    assert second != dsecond
    init2 = deep_cached_func()
    assert init == init2

    # verify that recompute="_all" recomputes nested functions.
    first = deep_cached_func3()
    second = deep_cached_func3(recompute="_all", cache_mode='overwrite')
    assert first != second

    # verify that recompute=[f,g] recomputes both f and g.
    first = deep_cached_func3()
    second = deep_cached_func3(recompute=["deep_cached_func3", "deep_cached_func2"], cache_mode='overwrite')
    assert first != second

    # verify that recompute=[f,g] doesn't recompute anything but f and g.
    first = deep_cached_func3()
    second = deep_cached_func3(recompute=["deep_cached_func3", "deep_cached_func1"], cache_mode='overwrite')
    assert first == second

def test_force_overwrite():
    # Test force overwrite here
    pass
