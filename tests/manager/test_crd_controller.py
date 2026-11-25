"""Tests for CRD readiness handling in CrdController.

Kubernetes condition status is the *string* "True"/"False"/"Unknown" —
truthiness checks on it are always wrong.
"""

import types

import pytest
from lightkube.models.apiextensions_v1 import (
    CustomResourceDefinitionCondition,
    CustomResourceDefinitionNames,
    CustomResourceDefinitionSpec,
    CustomResourceDefinitionStatus,
    CustomResourceDefinitionVersion,
)
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.apiextensions_v1 import CustomResourceDefinition
from lightkube.resources.core_v1 import Pod

from chopf.manager.controllers.crd import CrdController, crd_is_ready
from chopf.resources.registry import CustomResourceRegistry


def make_crd(*condition_statuses):
    conditions = [
        CustomResourceDefinitionCondition(type='NamesAccepted', status=status)
        for status in condition_statuses
    ]
    return CustomResourceDefinition(
        metadata=ObjectMeta(name='pods.test.group'),
        spec=CustomResourceDefinitionSpec(
            group='test.group',
            names=CustomResourceDefinitionNames(kind='Pod', plural='pods'),
            scope='Namespaced',
            versions=[
                CustomResourceDefinitionVersion(name='v1', served=True, storage=True)
            ],
        ),
        status=CustomResourceDefinitionStatus(conditions=conditions),
    )


def test_crd_is_ready_with_true_condition():
    assert crd_is_ready(make_crd('True')) is True


def test_crd_is_ready_with_false_condition():
    # 'False' is a non-empty string: truthy. The bug this guards against.
    assert crd_is_ready(make_crd('False')) is False


def test_crd_is_ready_with_unknown_condition():
    assert crd_is_ready(make_crd('Unknown')) is False


def test_crd_is_not_ready_without_conditions():
    assert crd_is_ready(make_crd()) is False


class FakeClient:
    def __init__(self, crd):
        self._crd = crd

    async def get(self, request):
        return self._crd


class ManagerStub:
    def __init__(self, crd):
        self.custom_resources = CustomResourceRegistry()
        self.resources = {Pod}
        self.added = []
        self.removed = []
        # Pre-register the CRD and a resource class for it.
        self.custom_resources.add(crd)
        self.custom_resources.register_resource(crd, crd.spec.versions[0], Pod)

    async def add_resource(self, resource):
        self.added.append(resource)

    async def remove_resource(self, resource):
        self.removed.append(resource)


def make_controller(crd):
    manager = ManagerStub(crd)
    controller = CrdController(
        manager,
        FakeClient(crd),
        object(),
        types.SimpleNamespace(get_informers_by=lambda resource: []),
        CustomResourceDefinition,
    )
    return controller, manager


@pytest.mark.anyio
async def test_ready_crd_is_added():
    crd = make_crd('True')
    controller, manager = make_controller(crd)

    await controller.reconcile(FakeClient(crd), request=None)

    assert manager.added == [Pod]
    assert manager.removed == []


@pytest.mark.anyio
async def test_unready_crd_is_not_added():
    crd = make_crd('False')
    controller, manager = make_controller(crd)

    await controller.reconcile(FakeClient(crd), request=None)

    assert manager.added == []
    assert manager.removed == []
