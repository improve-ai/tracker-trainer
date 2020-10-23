"""
Custom exceptions raised across the program.
-------------------------------------------------------------------------------
"""

class InvalidType(Exception):
    """ Raised if a record has a type different than 'decision' or 'rewards """
    def __str__(self):
        return "`type` must be 'decision' or 'rewards'"
