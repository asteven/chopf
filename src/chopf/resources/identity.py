"""Resource identity: apiVersion/kind/plural/namespaced — one access point.

Everything in chopf that needs to know what a resource *is* goes through
this module. It is currently backed by lightkube internals
(``lkr.api_info`` / ``_api_info``); when the backend changes (lightkube
v1 / msgspec), this module is the adapter boundary — call sites do not.

We deliberately do NOT monkey-patch lightkube's classes. chopf's own
resources (created via the @resource decorator) get their dunders from
chopf's Resource base class; lightkube-provided resources are formatted
for logging via resource_repr()/resource_str().
"""

from lightkube.core import resource as lkr


def api_info(resource):
    """Group/version/kind, plural and verbs of a resource class or instance."""
    return lkr.api_info(resource)


def api_version(resource) -> str:
    return api_info(resource).resource.api_version


def kind(resource) -> str:
    return api_info(resource).resource.kind


def gvk(resource) -> str:
    return f'{api_version(resource)}/{kind(resource)}'


def is_namespaced(resource) -> bool:
    return issubclass(resource, (lkr.NamespacedResource, lkr.NamespacedSubResource))


def resource_str(obj) -> str:
    """'apiVersion/kind' of a resource instance.

    Falls back to api_info for locally constructed objects whose
    apiVersion/kind fields are not set yet (they default to None; the
    client fills them on write, server payloads carry them on read).
    """
    try:
        api_version = obj.apiVersion
        kind = obj.kind
    except AttributeError:
        return repr(obj)
    if not api_version or not kind:
        try:
            info = api_info(obj).resource
            api_version = api_version or info.api_version
            kind = kind or info.kind
        except AttributeError:
            pass
    return f'{api_version}/{kind}'


def resource_repr(obj) -> str:
    """Human readable identity of a resource instance, for logging."""
    try:
        ident = [resource_str(obj)]
        metadata = obj.metadata
        name = getattr(metadata, 'name', None)
        namespace = getattr(metadata, 'namespace', None)
        resource_version = getattr(metadata, 'resourceVersion', None)
        if namespace is not None:
            ident.append(f'{namespace}/{name}')
        elif name is not None:
            ident.append(name)
        if resource_version is not None:
            ident.append(resource_version)
        return f'<Object {" ".join(ident)}>'
    except AttributeError:
        return repr(obj)
