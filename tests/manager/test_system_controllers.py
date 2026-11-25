"""Tests for the subclass front-end and the internal runtime.

Seams under test:
- ControllerSpec.from_class: a Controller subclass declares class
  attributes (resource, concurrent_reconciles, wait_for_cache) which
  become the frozen spec.
- InternalRuntime: materializes the built-in controllers with the
  runtime's own cache and clients — never the manager's shared cache —
  so a follower can run them without paying for the leader payload.
"""

import importlib

import pytest
from lightkube.resources.apiextensions_v1 import CustomResourceDefinition
from lightkube.resources.core_v1 import Namespace

manager_mod = importlib.import_module('chopf.manager.manager')
builders_mod = importlib.import_module('chopf.manager.builders')
system_mod = importlib.import_module('chopf.manager.system')

from chopf.controller import Controller
from chopf.manager.controllers import CrdController
from chopf.specs import ControllerSpec


class DummyCache:
    def __init__(self, api_client, stores=None):
        self.api_client = api_client
        self._all_namespaces = False
        self.namespaces = set()
        self.resources = set()
        self._stores = stores if stores is not None else {}

    def get_store(self, resource):
        return self._stores.setdefault(resource, object())


class DummyClient:
    namespace = 'default'

    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture(autouse=True)
def patch_manager_dependencies(monkeypatch):
    monkeypatch.setattr(manager_mod, 'LightkubeAsyncClient', DummyClient)
    monkeypatch.setattr(manager_mod, 'Cache', DummyCache)
    monkeypatch.setattr(system_mod, 'Cache', DummyCache)
    monkeypatch.setattr(builders_mod, 'resource_rbac', lambda *a, **kw: None)
    monkeypatch.setattr(manager_mod, 'resource_rbac', lambda *a, **kw: None)


@pytest.fixture
def manager():
    return manager_mod.Manager()


def test_from_class_reads_declared_attributes():
    spec = ControllerSpec.from_class(CrdController)

    assert spec.controller_class is CrdController
    assert spec.resource is CustomResourceDefinition
    assert spec.concurrent_reconciles == 4
    # The internal runtime's cache is always running before its
    # controllers start, so they wait for it to be synced.
    assert spec.wait_for_cache is True
    # Handlers are methods on the instance, not spec fields.
    assert spec.reconcile is None


def test_from_class_requires_resource_declaration():
    class Broken(Controller):
        pass

    with pytest.raises(TypeError, match='resource'):
        ControllerSpec.from_class(Broken)


def test_internal_runtime_setup_materializes_controllers(manager):
    manager._setup_runtime()
    runtime = system_mod.InternalRuntime(manager)
    runtime._setup_controllers()

    # CRD controller always; namespace controller only in namespaced mode.
    assert set(runtime.controllers) == {CustomResourceDefinition, Namespace}

    crd_controller = runtime.controllers[CustomResourceDefinition]
    assert isinstance(crd_controller, CrdController)
    # The manager is an explicit constructor dependency.
    assert crd_controller.manager is manager
    # The runtime's own cache and clients — NOT the manager's.
    assert crd_controller.cache is runtime.cache
    assert crd_controller.cache is not manager.cache
    assert crd_controller.async_client is runtime.async_client
    # Internal controllers do not take part in payload membership.
    assert CustomResourceDefinition not in manager._controllers
    # Static cache membership: exactly the internal resources.
    runtime._configure_cache()
    assert runtime.cache.resources == {CustomResourceDefinition, Namespace}


def test_internal_runtime_setup_skips_namespace_controller_when_cluster_wide(manager):
    manager.all_namespaces = True
    runtime = system_mod.InternalRuntime(manager)
    runtime._setup_controllers()

    assert set(runtime.controllers) == {CustomResourceDefinition}


def test_materialize_class_based_controller(manager):
    spec = ControllerSpec.from_class(CrdController)
    controller = manager._materialize_controller(spec)

    assert isinstance(controller, CrdController)
    # The manager is an explicit constructor dependency.
    assert controller.manager is manager
    assert controller.concurrent_reconciles == 4
    assert controller.wait_for_cache is True
    # Registered for membership-driven start/stop.
    assert manager._controllers[CustomResourceDefinition] is controller


def test_materialize_builder_spec_still_works(manager):
    builder = manager.controller(CustomResourceDefinition)
    builder.reconcile()(lambda client, request: None)
    controller = manager._materialize_controller(builder.to_spec())

    assert type(controller) is Controller
    assert manager._controllers[CustomResourceDefinition] is controller
