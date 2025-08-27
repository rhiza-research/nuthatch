from abc import ABC, abstractmethod
from inspect import signature

class NuthatchProcessor(ABC):
    """Decorator example mixing class and function definitions."""
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):

        params = signature(self.func).parameters
        args, kwargs = self.process_arguments(params, args, kwargs)

        data = self.func(*args, **kwargs)

        if not self.validate_data(data):
            if 'force_overwrite' in kwargs and kwargs['force_overwrite']:
                print("Data validation failed and forceoverwrite set. Overwriting the result.")
                kwargs['recompute'] = True #TODO - does this mess with recompute?
                data = self.func(*args, **kwargs)
            else:
                inp = input("""Data failed validation. Would you like to overwrite the result (y/n)?""")
                if inp == 'y' or inp == 'Y':
                    kwargs['recompute'] = True #TODO - does this mess with recompute?
                    kwargs['force_overwrite'] = True #TODO - does this mess with recompute?
                    data = self.func(*args, **kwargs)

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
