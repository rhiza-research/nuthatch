# Nuthatch

Nuthatch is a tool for building pure-python big data pipelines. At its core it
enables the transparent multi-level caching and recall of results, tracking the 
dependencies of a result at runtime and automatically
recomputing results as those dependencies change. It supports a variety of 
common storage backends, data processing frameworks, and their associated
data types for caching. 

It also provides a framework for re-using and sharing data-type specific pre-processing,
post-processing, argument specification, and argument validation, and for these data type
processors to pass hints to storage backends for more efficient storager and recall.

Nuthatch was created to alleviate the comon pattern of data processing pipelines manually
specifying their output storage locations, and the requirements of pipeline builders to
use external data orchestration tools to specify the execution of their pipeliness. With Nuthatch
simply tag your functions and anyone who has access to your storage backend - you, your
team, or the public - can acess and build off of your most up-to-date data.

## How it works - a minimal example

Nuthatch starts like any other memoization and caching tool.
```
from nuthatch import cacheable

@cacheable(cache_args=['name'])
def hello(name):
    return "Hello " + name + '!'

hello('nuthatch') # The string "Hello nuthatch!" is stored in a pkl file
hello('nuthatch') # The string "Hello nuthatch!" is recalled from the pkl file rather than recomputing
hello('nuthatch', memoize=True) # The string "Hello nuthatch!" is also stored in memory
hello('chickadee') # The string "Hello chickadee!" is stored in a new pkl file
```

Nuthach's power comes in several flavors
