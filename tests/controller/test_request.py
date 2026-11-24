import itertools

import pytest
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources.core_v1 import Pod

from chopf.controller.request import (
    Request,
    request_for_object,
    request_for_owner,
    requests_from_event_for_object,
    requests_from_event_for_owner,
)
from chopf.cache import events


@pytest.fixture(autouse=True)
def ensure_pod_defaults():
    Pod.apiVersion = 'v1'
    Pod.kind = 'Pod'


def build_pod(name: str, namespace: str | None = None):
    metadata = ObjectMeta(name=name, namespace=namespace)
    return Pod(metadata=metadata)


def build_owner_ref(name: str, controller: bool = True):
    return OwnerReference(
        apiVersion='v1',
        kind='Pod',
        name=name,
        uid='uid-' + name,
        controller=controller,
    )


def test_request_properties_and_equality():
    req1 = Request(Pod, 'foo', namespace='ns')
    req2 = Request(Pod, 'foo', namespace='ns')
    req3 = Request(Pod, 'bar', namespace='ns')

    assert req1.api_version == 'v1'
    assert req1.kind == 'Pod'
    assert req1 == req2
    assert hash(req1) == hash(req2)
    assert req1 != req3
    assert 'foo' in repr(req1)


def test_request_for_object_yields_values():
    pod = build_pod('foo', 'ns')
    requests = list(request_for_object(pod))
    assert len(requests) == 1
    assert requests[0].name == 'foo'
    assert requests[0].namespace == 'ns'


def test_request_for_owner_respects_controller_flag():
    owner = Pod
    child = build_pod('child', 'ns')
    child.metadata.ownerReferences = [
        build_owner_ref('ignored', controller=False),
        build_owner_ref('owner'),
    ]

    requests = list(request_for_owner(child, owner=owner))
    assert len(requests) == 1
    assert requests[0].name == 'owner'


def test_requests_from_event_for_object_handles_update_event():
    pod_old = build_pod('old', 'ns')
    pod_new = build_pod('new', 'ns')
    event = events.UpdateEvent(old=pod_old, new=pod_new)

    requests = list(requests_from_event_for_object(event))
    names = {req.name for req in requests}
    assert names == {'old', 'new'}


def test_requests_from_event_for_owner_handles_create_event():
    owner = Pod
    child = build_pod('child', 'ns')
    child.metadata.ownerReferences = [build_owner_ref('owner')]
    event = events.CreateEvent(obj=child)

    requests = list(requests_from_event_for_owner(event, owner=owner))
    assert len(requests) == 1
    assert requests[0].name == 'owner'
