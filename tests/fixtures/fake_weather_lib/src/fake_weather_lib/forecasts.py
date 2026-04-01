from nuthatch import cache


@cache(cache_args=['key'])
def fake_weather_cached_fn(key='default'):
    """Test-only cached function for config resolution tests."""
    return {'key': key, 'pkg': 'fake_weather_lib'}
