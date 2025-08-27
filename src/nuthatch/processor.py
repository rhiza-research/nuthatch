from abc import ABC, abstractmethod
from inspect import signature

class NutachProcessor(ABC):
    """Decorator example mixing class and function definitions."""
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):

        params = signature(self.func).parameters
        args, kwargs = self.validate_arguments(params, args, kwargs)

        data = self.func(*args, **kwargs)

        if not self.validate_data(data):
            kwargs['recompute'] = True #TODO - does this mess with recompute?
            self.func(*args, **kwargs)

        data = self.post_process(data)

        return data

    @abstractmethod
    def post_process(self, data):
        # Do something to the data and return it
        return data

    def process_arguments(self, params, args, kwargs):
        return args, kwargs

    def validate_data(self, data):
        return True

# example use of nutach processor
#def my_dec_factory(param1, param2):
#    def decorator(func):
#         return MyDecorator(func, param1, param2)
#    return decorator
