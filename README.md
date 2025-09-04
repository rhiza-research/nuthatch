# Nuthatch

Nuthatch is a tool for building pure-python big data pipelines. At its core it
enables the transparent multi-level caching and recall of results in formats that 
are efficient for each data type. It supports a variety of 
common storage backends, data processing frameworks, and their associated
data types for caching. 

It also provides a framework for re-using and sharing data-type specific
post-processing, and for these data type
processors to pass hints to storage backends for more efficient storager and recall.

Nuthatch was created to alleviate the comon pattern of data processing pipelines manually
specifying their output storage locations, and the requirements of pipeline builders to
use external data orchestration tools to specify the execution of their pipeliness. With Nuthatch
simply tag your functions and anyone who has access to your storage backend - you, your
team, or the public - can acess and build off of your most up-to-date data.
