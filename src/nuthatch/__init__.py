"""
Nuthatch is a library for caching data based on the function call and its arguments.

It caches data in a variety of backends optimized for different data types.
"""
from .config import config_parameter, set_parameter
from .nuthatch import cache

# Trigger backend registration
import nuthatch.backends #noqa

__all__ = ["set_parameter", "config_parameter", "cache"]
