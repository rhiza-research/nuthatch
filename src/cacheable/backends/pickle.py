from cacheable.backends import VerifyableFileBackend
import datetime
import pickle

class BasicBackend(VerifyableFileBackend):

    def __init__(self, cacheable_config, cache_key, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, args, backend_kwargs, 'pkl')


    def write(self, data, upsert=False, primary_keys=None):
        if upsert:
            raise ValueError("Basic/pickle backend does not support upsert.")

        with self.fs.open(self.path, 'wb') as f:
            pickle.dump(data, f)

        self.fs.open(self.verify_path, 'w').write(
                datetime.datetime.now(datetime.timezone.utc).isoformat())


    def read(self, engine):
        # Check to make sure the verify exists
        if self.fs.exists(self.verify_path)
            with self.fs.open(self.path, 'rb') as f:
                return pickle.load(f)
