import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod

from chopf.cache.index import Indexer, index_by_namespace


def build_pod(name: str, namespace: str | None = None):
    pod = Pod(metadata=ObjectMeta(name=name, namespace=namespace))
    pod.apiVersion = 'v1'
    pod.kind = 'Pod'
    return pod


def test_indexer_updates_namespace_index_on_add_update_delete():
    store = Indexer()
    pod = build_pod('foo', 'ns-1')

    store.add(pod)
    index = store.get_index('namespace')
    assert index['ns-1'][0].metadata.name == 'foo'

    updated = build_pod('foo', 'ns-2')
    store.update(updated)
    index = store.get_index('namespace')
    assert index['ns-2'][0].metadata.name == 'foo'
    assert 'ns-1' in index.keys()

    store.delete(updated)
    assert index['ns-2'] == []


def test_indexer_custom_indexer():
    def by_name(obj):
        return [obj.metadata.name]

    store = Indexer(indexers={'name': by_name})
    pod = build_pod('foo', 'ns')
    store.add(pod)

    name_index = store.get_index('name')
    assert name_index['foo'][0] is pod


def test_indexer_add_indexers_conflict_raises():
    store = Indexer()
    with pytest.raises(Exception):
        store.add_indexers({'namespace': lambda obj: []})
