"""Tests for the pure membership planner.

Seam under test: plan_informers() — given the desired set of resources
and namespaces and the currently running informers, which informers must
start and which must stop? Pure in, pure out: no anyio, no lightkube.
"""
from chopf.membership import (
    ALL_NAMESPACES,
    InformerRef,
    ResourceRef,
    plan_informers,
)

POD = ResourceRef(api_version='v1', kind='Pod', namespaced=True)
NS = ResourceRef(api_version='v1', kind='Namespace', namespaced=False)
CM = ResourceRef(api_version='v1', kind='ConfigMap', namespaced=True)


def plan(resources, namespaces, current=(), all_namespaces=False, extra_desired=()):
    return plan_informers(
        frozenset(resources),
        frozenset(namespaces),
        frozenset(current),
        all_namespaces=all_namespaces,
        extra_desired=frozenset(extra_desired),
    )


def test_namespaced_resource_wants_one_informer_per_namespace():
    result = plan([POD], ['a', 'b'])
    assert result.start == {
        InformerRef(POD, 'a'),
        InformerRef(POD, 'b'),
    }
    assert result.stop == set()


def test_cluster_scoped_resource_wants_single_global_informer():
    result = plan([NS], ['a', 'b'])
    assert result.start == {InformerRef(NS, None)}


def test_all_namespaces_collapses_to_star_informer():
    result = plan([POD], [], all_namespaces=True)
    assert result.start == {InformerRef(POD, ALL_NAMESPACES)}


def test_added_namespace_starts_new_informer():
    current = [InformerRef(POD, 'a')]
    result = plan([POD], ['a', 'b'], current=current)
    assert result.start == {InformerRef(POD, 'b')}
    assert result.stop == set()


def test_removed_namespace_stops_its_informer():
    current = [InformerRef(POD, 'a'), InformerRef(POD, 'b')]
    result = plan([POD], ['a'], current=current)
    assert result.start == set()
    assert result.stop == {InformerRef(POD, 'b')}


def test_removed_resource_stops_all_its_informers():
    current = [InformerRef(POD, 'a'), InformerRef(CM, 'a'), InformerRef(NS, None)]
    result = plan([CM], ['a'], current=current)
    assert result.stop == {InformerRef(POD, 'a'), InformerRef(NS, None)}


def test_no_change_plans_nothing():
    current = [InformerRef(POD, 'a'), InformerRef(NS, None)]
    result = plan([POD, NS], ['a'], current=current)
    assert result.start == set()
    assert result.stop == set()


def test_extra_desired_pins_informer_for_unmanaged_resource():
    """System resources are not part of membership, but their informers
    can be pinned explicitly (e.g. the Lease in the manager namespace)."""
    lease = ResourceRef(api_version='coordination.k8s.io/v1', kind='Lease', namespaced=True)
    pinned = InformerRef(lease, 'manager-ns')
    result = plan([], ['a'], extra_desired=[pinned])
    assert result.start == {pinned}
    # And it is never planned away while pinned.
    result = plan([], ['a'], current=[pinned], extra_desired=[pinned])
    assert result.stop == set()


def test_unpinned_extra_desired_is_stopped():
    lease = ResourceRef(api_version='coordination.k8s.io/v1', kind='Lease', namespaced=True)
    pinned = InformerRef(lease, 'manager-ns')
    result = plan([], ['a'], current=[pinned], extra_desired=[])
    assert result.stop == {pinned}
