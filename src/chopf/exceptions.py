from lightkube.core.exceptions import ApiError

__all__ = [
    'ApiError',
    'Error',
    'ObjectError',
    'ObjectNotFound',
    'PermanentError',
    'Requeue',
    'StoreKeyError',
    'TemporaryError',
]


class Error(Exception):
    """Base class for all custom Exceptions."""

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.message}'


class ObjectError(Error):
    def __init__(self, obj):
        self.obj = obj

    def __repr__(self):
        obj = self.obj
        kind = getattr(obj, 'kind', None)
        api_version = getattr(obj, 'apiVersion', None)
        namespace = getattr(obj.metadata, 'namespace', None)
        name = obj.metadata.name
        out = []
        if api_version is not None and kind is not None:
            out.append(f'{api_version}/{kind}')
        if namespace is not None:
            out.append(f'{namespace}/{name}')
        else:
            out.append(name)
        msg = ' '.join(out)
        return f'{self.__class__.__name__}: {msg}'


class ObjectNotFound(ObjectError):
    pass


class StoreKeyError(ObjectError):
    pass


class TemporaryError(Error):
    """Raised by a reconcile function when a recoverable error occurs.
    The request will be requeued after the given delay."""

    def __init__(self, message=None, delay=10):
        super().__init__(message)
        self.delay = delay

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.message} delay: {self.delay}'


class PermanentError(Error):
    """Raised by a reconcile function when a non-recoverably error occurs."""


class Requeue(Error):
    """Raised by a reconcile function to requeue a request.
    The request will be requeued after the given delay."""

    def __init__(self, after=None):
        self.after = after

    def __repr__(self):
        return f'{self.__class__.__name__}: after: {self.after}'
