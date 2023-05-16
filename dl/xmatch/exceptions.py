"""
Provides various cross match exceptions
"""

class XMatchException(Exception):
    """
    Define a new xmatch query exception
    """
    message = "XMatch query error encountered"

    def __init__(self, message=None):
        """
        Xmatch Query Exception
        """
        super().__init__(self.__class__.message if not message else message)


class ConfigMissing(XMatchException):
    """
    Indicates that the required config is missing from a call
    """
    message = 'No configuration  file was provided.'

