import msgspec
from lightkube.resources.core_v1 import Pod

from chopf.resources import ObjectMeta, normalize_metadata


def make_pod(raw: bytes | None = None) -> Pod:
    if raw is None:
        raw = b'{"apiVersion":"v1","kind":"Pod","metadata":{"name":"foo","namespace":"default"}}'
    return msgspec.json.decode(raw, type=Pod)


def test_decoded_lightkube_resource_has_none_collections():
    # Baseline: lightkube's built-in models decode absent collections as None.
    pod = make_pod()
    assert pod.metadata.annotations is None
    assert pod.metadata.labels is None
    assert pod.metadata.finalizers is None
    assert pod.metadata.ownerReferences is None


def test_normalize_metadata_defaults_absent_collections():
    pod = normalize_metadata(make_pod())
    assert pod.metadata.annotations == {}
    assert pod.metadata.labels == {}
    assert pod.metadata.finalizers == []
    assert pod.metadata.ownerReferences == []


def test_normalize_metadata_keeps_existing_values():
    pod = make_pod(
        b'{"metadata":{"name":"foo","annotations":{"a":"b"},"labels":{"x":"y"},'
        b'"finalizers":["mine"],"ownerReferences":['
        b'{"apiVersion":"v1","kind":"Pod","name":"owner","uid":"1"}]}}'
    )
    pod = normalize_metadata(pod)
    assert pod.metadata.annotations == {'a': 'b'}
    assert pod.metadata.labels == {'x': 'y'}
    assert pod.metadata.finalizers == ['mine']
    assert len(pod.metadata.ownerReferences) == 1
    assert pod.metadata.ownerReferences[0].name == 'owner'


def test_normalize_metadata_does_not_materialize_managed_fields():
    # managedFields is never mutated by operators and is often the largest
    # field on an object; materializing it would be pure waste.
    pod = normalize_metadata(make_pod())
    assert pod.metadata.managedFields is None


def test_normalize_metadata_tolerates_missing_metadata():
    class NoMetadata:
        metadata = None

    obj = NoMetadata()
    assert normalize_metadata(obj) is obj


def test_normalized_object_roundtrips_empty_maps():
    # Documented trade-off: normalized fields are re-encoded as empty
    # maps/lists rather than being omitted. Kubernetes treats empty maps
    # as absent, so this is benign.
    pod = normalize_metadata(make_pod())
    encoded = msgspec.json.encode(pod)
    decoded = msgspec.json.decode(encoded, type=Pod)
    assert decoded.metadata.annotations == {}
    assert decoded.metadata.labels == {}


def test_chopf_object_meta_uses_same_fields():
    # chopf's own ObjectMeta sugar and normalize_metadata must not diverge.
    metadata = ObjectMeta(name='foo')
    assert metadata.annotations == {}
    assert metadata.labels == {}
    assert metadata.finalizers == []
    assert metadata.ownerReferences == []
    assert metadata.managedFields is None
