from lightkube.core import resource as lkr
from lightkube.core.exceptions import ApiError

__all__ = [
    'ApiError',
    'Error',
    'FatalError',
    'HttpError',
    'ObjectError',
    'ObjectNotFound',
    'ApiObjectNotFound',
    'PermanentError',
    'Requeue',
    'StoreKeyError',
    'TemporaryError',
    'iterate_errors',
]


def iterate_errors(exc):
    """
    iterate over all non-exceptiongroup parts of an exception(group)
    """
    if isinstance(exc, BaseExceptionGroup):
        for e in exc.exceptions:
            yield from iterate_errors(e)
    else:
        yield exc


class FatalError(Exception):
    """A fatal error that we can not recover from."""


class Error(Exception):
    """Base class for all custom Exceptions."""

    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.message}'


class HttpError(Error):
    """An error that occured on the transport level while talking to the api.
    """
    def __init__(self, http_method, url, status_code, message=None):
        self.http_method = http_method
        self.url = url
        self.status_code = status_code
        self.message = message

    def __str__(self):
        if self.message:
            return self.message
        else:
            return '{0} to {1} failed with status: {2}'.format(
                self.http_method, self.url, self.status_code
            )


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


class ApiObjectNotFound(ObjectNotFound):
    def __init__(self, resource, name, namespace=None):
        self.resource = resource
        self.name = name
        self.namespace = namespace

    def __repr__(self):
        info = lkr.api_info(self.resource)
        kind = info.resource.kind
        api_version = info.resource.api_version
        namespace = self.namespace
        name = self.name
        out = []
        if api_version is not None and kind is not None:
            out.append(f'{api_version}/{kind}')
        if namespace is not None:
            out.append(f'{namespace}/{name}')
        else:
            out.append(name)
        msg = ' '.join(out)
        return f'{self.__class__.__name__}: {msg}'


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
