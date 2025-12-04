"""
This module contains the NuthatchProcessor class, which is a base class for all Nuthatch processors.
"""
from abc import ABC, abstractmethod
from inspect import signature
import functools

import logging
logger = logging.getLogger(__name__)

class NuthatchProcessor(ABC):
    """
    Base class for all Nuthatch processors.

    A NuthatchProcessor is a class that wraps a function and provides a way to process the function's arguments,
    post-process the function's return value, and validate the function's return value.

    It can be used in combination with cacheable to, for instance, post process cached values or inject
    repeated arguments into a cacheable function.
    """
    def __init__(self, **kwargs):
        """
        Initialize a processor.

        Args:
            func: The function to wrap.
            validate_data: True to automatically validate data if supported
        """

        self.validate_data = kwargs.get('validate_data', False)

    def __call__(self, func, ):
        """
        Call the wrapped function with the given arguments.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract validate data if present
            if 'validate_data' in kwargs:
                passed_validate_data = kwargs['validate_data']
                if passed_validate_data:
                    self.validate_data = passed_validate_data
                del kwargs['validate_data']

            # Allow process to process the arguments
            params = signature(func).parameters
            args, kwargs = self.process_arguments(params, args, kwargs)

            # Call the function
            data = func(*args, **kwargs)

            # Validate data if requested
            if self.validate_data and not self.validate(data):
                raise ValueError("Data failed validation. Please manually overwrite data to proceed.")

                #if 'force_overwrite' in kwargs and kwargs['force_overwrite']:
                #    logger.info("Data validation failed and force_overwrite set. Overwriting the result.")
                #    kwargs['recompute'] = True #TODO - does this mess with recompute?
                #    data = self.func(*args, **kwargs)
                #else:
                #    inp = input("""Data failed validation. Would you like to overwrite the result (y/n)?""")
                #    if inp == 'y' or inp == 'Y':
                #        if 'recompute' in kwargs:
                #            if isinstance(kwargs['recompute'], str):
                #                kwargs['recompute'] = [kwargs['recompute'], self.func.__name__]
                #            elif isinstance(kwargs['recompute'], list):
                #                kwargs['recompute'] = kwargs['recompute'].append(self.func.__name__)
                #        else:
                #            kwargs['recompute'] = True #TODO - does this mess with recompute?

                #        kwargs['force_overwrite'] = True #TODO - does this mess with recompute?
                #        data = self.func(*args, **kwargs)

            data = self.post_process(data)

            return data
        return wrapper


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
    def validate(self, data):
        """
        Validate the data. Used to trigger recomputation if the data is invalid.

        Args:
            data: The data to validate.

        Returns:
            True if the data is valid, False otherwise.
        """
        return True
