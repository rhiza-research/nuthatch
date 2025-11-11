import pickle
from nuthatch.backend import FileBackend, register_backend

@register_backend
class BasicBackend(FileBackend):
    """
    Basic backend for caching data in a pickle file.
    """

    backend_name = "basic"

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, extension='pkl')


    def write(self, data):
        with self.fs.open(self.path, 'wb') as f:
            pickle.dump(data, f)

        return data


    def upsert(self, data, upsert_keys=None):
        raise NotImplementedError("Basic backend does not support upsert.")


    def read(self, engine=None):
        # Check to make sure the verify exists
        with self.fs.open(self.path, 'rb') as f:
            return pickle.load(f)
