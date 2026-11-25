"""Characterization tests for chopf.resources.identity — the single
access point for resource identity — and for the de-monkeypatching of
lightkube's classes.

Seam under test: identity helpers over a lightkube-provided resource
(Pod) and over a chopf resource created by the @resource decorator.
"""
import lightkube.core.resource as lkr
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf import crd
from chopf.resources import ObjectMeta as ChopfObjectMeta
from chopf.resources import identity


@crd.resource(group='tests.chopf', version='v1', scope='Namespaced')
class Widget:
    metadata: ChopfObjectMeta
    spec: dict = None


def make_pod(name='foo', namespace='default', resource_version='1'):
    pod = Pod(
        metadata=ObjectMeta(
            name=name, namespace=namespace, resourceVersion=resource_version
        )
    )
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    return pod


def test_api_version_and_kind():
    assert identity.api_version(Pod) == 'v1'
    assert identity.kind(Pod) == 'Pod'
    assert identity.gvk(Pod) == 'v1/Pod'


def test_is_namespaced():
    assert identity.is_namespaced(Pod) is True
    assert identity.is_namespaced(Widget) is True


def test_resource_repr_includes_identity():
    pod = make_pod()
    assert identity.resource_repr(pod) == '<Object v1/Pod default/foo 1>'
    assert identity.resource_str(pod) == 'v1/Pod'


def test_resource_repr_tolerates_plain_objects():
    assert identity.resource_repr(object()).startswith('<object object at')


def test_chopf_resources_have_readable_dunders():
    widget = Widget(metadata=ChopfObjectMeta(name='w', namespace='ns'))
    assert str(widget) == 'tests.chopf/v1/Widget'
    assert repr(widget) == '<Object tests.chopf/v1/Widget ns/w>'


def test_lightkube_classes_are_not_monkeypatched():
    """The global lkr.Resource patch is gone: lightkube's own dunders
    stay intact, chopf's formatting lives in identity + chopf's base."""
    assert lkr.Resource.__repr__ is not identity.resource_repr
    assert '<Object' not in repr(make_pod())
