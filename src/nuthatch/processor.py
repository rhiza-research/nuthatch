"""
This module contains the NuthatchProcessor class, which is a base class for all Nuthatch processors.
"""
from abc import ABC, abstractmethod
from inspect import signature

class NuthatchProcessor(ABC):
    """
    Base class for all Nuthatch processors.

    A NuthatchProcessor is a class that wraps a function and provides a way to process the function's arguments,
    post-process the function's return value, and validate the function's return value.
    """
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        """
        Call the wrapped function with the given arguments.
        """
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
        """
        Post-process the data.

        Args:
            data: The data to post-process.

        Returns:
            The post-processed data.

        Raises:
            ValueError: If the data is of the wrong type.
        """
        return data

    def process_arguments(self, params, args, kwargs):
        """
        Process the arguments.

        Args:
            params: The parameters of the function.
            args: The arguments to the function.
            kwargs: The keyword arguments to the function.

        Returns:
            The processed arguments and keyword arguments to be passed to the function.
        """
        return args, kwargs

    @abstractmethod
    def validate_data(self, data):
        """
        Validate the data. Used to trigger recomputation if the data is invalid.

        Args:
            data: The data to validate.

        Returns:
            True if the data is valid, False otherwise.
        """
        return True

# example use of nutach processor
#def my_dec_factory(param1, param2):
#    def decorator(func):
#         return MyDecorator(func, param1, param2)
#    return decorator
