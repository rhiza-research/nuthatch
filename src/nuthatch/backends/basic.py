from nuthatch.backend import FileBackend, register_backend
import pickle

@register_backend
class BasicBackend(FileBackend):
    """
    Basic backend for caching data in a pickle file.
    """

    backend_name = "basic"

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs, 'pkl')


    def write(self, data, upsert=False, primary_keys=None):
        if upsert:
            raise ValueError("Basic/pickle backend does not support upsert.")

        with self.fs.open(self.path, 'wb') as f:
            pickle.dump(data, f)


    def read(self, engine=None):
        # Check to make sure the verify exists
        if self.fs.exists(self.path):
            with self.fs.open(self.path, 'rb') as f:
                return pickle.load(f)
