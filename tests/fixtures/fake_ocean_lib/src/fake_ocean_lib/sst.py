from nuthatch import cache


@cache(cache_args=['key'])
def fake_ocean_cached_fn(key='default'):
    """Test-only cached function for config resolution tests."""
    return {'key': key, 'pkg': 'fake_ocean_lib'}
