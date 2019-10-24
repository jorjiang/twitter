import logging

logger = logging.getLogger(__name__)


class DbConnectionSingleton(type):
    """
    Define an Instance operation that lets clients access its unique
    instance.
    """

    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instances = {}

    def __call__(cls, *args, **kwargs):
        connection = False
        for arg in args:
            connection = arg
            break
        if not connection in cls._instances:
            logger.debug("create new connection {}".format(connection))
            cls._instances[connection] = super().__call__(*args, **kwargs)
        else:
            logger.debug("return existing connection {}".format(connection))

        return cls._instances[connection]

    def __del__(cls):
        del cls._instances
