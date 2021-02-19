"""
Custom exceptions raised across the program.
-------------------------------------------------------------------------------
"""

class InvalidTypeError(Exception):
    """ Raised if a record has a type different than 'decision' or 'rewards """
    def __str__(self):
        return "`type` must be 'decision' or 'rewards'"
class UpdateListenersError(Exception):
    """ Raised if a there is a problem when using the function UpdateListeners """
    def __str__(self):
        return "Problem when updating listeners."

class EnvirontmentVariableError(Exception):
    """ Raised if a there is a missing environment variable """
    def __str__(self):
        return "An environment variable is missing"
