import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.cache.store import Store, meta_namespace_key_func
from chopf.exceptions import StoreKeyError


def build_pod(name: str, namespace: str | None = None):
    metadata = ObjectMeta(name=name, namespace=namespace)
    pod = Pod(metadata=metadata)
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    return pod


def test_meta_namespace_key_func_namespaced():
    pod = build_pod('foo', 'ns')
    assert meta_namespace_key_func(pod) == 'ns/foo'


def test_meta_namespace_key_func_cluster_scoped():
    pod = build_pod('foo', None)
    assert meta_namespace_key_func(pod) == 'foo'


def test_meta_namespace_key_func_missing_metadata():
    pod = Pod(metadata=None)
    with pytest.raises(StoreKeyError):
        meta_namespace_key_func(pod)


def test_store_add_update_delete():
    pod = build_pod('foo', 'ns')
    store = Store()

    store.add(pod)
    assert store.get(pod) is pod

    updated = build_pod('foo', 'ns')
    updated.metadata.resourceVersion = '2'
    store.update(updated)
    assert store.get(updated).metadata.resourceVersion == '2'

    store.delete(updated)
    with pytest.raises(KeyError):
        store.get(updated)


def test_store_replace_and_clear():
    pods = [build_pod(f'pod-{i}', 'ns') for i in range(3)]
    store = Store()

    store.replace(pods)
    assert set(store.values()) == set(pods)

    store.clear()
    assert list(store.values()) == []
