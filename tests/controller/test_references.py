"""Tests for owner/controller reference helpers as pure functions."""

import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.controller import set_controller_reference, set_owner_reference


def make_pod(name, uid=None):
    pod = Pod(metadata=ObjectMeta(name=name, namespace='default', uid=uid))
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    return pod


def test_set_owner_reference_appends_reference():
    owner = make_pod('owner', uid='1234')
    subject = make_pod('subject')

    ref = set_owner_reference(owner, subject)

    assert ref in subject.metadata.ownerReferences
    assert ref.uid == '1234'
    assert ref.controller is False
    assert ref.blockOwnerDeletion is False


def test_set_controller_reference_sets_controller_flags():
    owner = make_pod('owner', uid='1234')
    subject = make_pod('subject')

    ref = set_controller_reference(owner, subject)

    assert ref.controller is True
    assert ref.blockOwnerDeletion is True


def test_set_controller_reference_rejects_second_controller():
    owner = make_pod('owner', uid='1234')
    other = make_pod('other', uid='5678')
    subject = make_pod('subject')
    set_controller_reference(owner, subject)

    with pytest.raises(Exception, match='controller'):
        set_controller_reference(other, subject)
