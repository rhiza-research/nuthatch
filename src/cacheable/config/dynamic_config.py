
dynamic_parameters = {}

def set_config_parameter(parameter_name):
    def decorator(function):
        dynamic_parameters[parameter_name] = function
    return decorator
