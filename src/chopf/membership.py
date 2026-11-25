"""Pure membership planning: which informers should exist?

The manager's dynamic state is (active resources × active namespaces).
From that, plus the informers currently running, this module computes a
plan: informers to start, informers to stop.

Everything here is pure — no anyio, no lightkube client, no I/O — so the
core of the manager's dynamic behavior is unit-testable in isolation.
Controllers reconcile; the manager *plans membership*.
"""

from __future__ import annotations

import dataclasses

ALL_NAMESPACES = '*'


@dataclasses.dataclass(frozen=True)
class ResourceRef:
    """Hashable identity of a resource type."""

    api_version: str
    kind: str
    namespaced: bool


@dataclasses.dataclass(frozen=True)
class InformerRef:
    """Hashable identity of an informer: a resource in a namespace."""

    resource: ResourceRef
    namespace: str | None


@dataclasses.dataclass(frozen=True)
class InformerPlan:
    start: frozenset
    stop: frozenset


def plan_informers(
    resources: frozenset,
    namespaces: frozenset,
    current: frozenset,
    all_namespaces: bool = False,
    extra_desired: frozenset = frozenset(),
) -> InformerPlan:
    """Compute which informers must start and which must stop.

    extra_desired pins informers that are not driven by membership
    (resources x namespaces) — e.g. the informers of the system
    controllers, which must exist regardless of the active set.
    """
    desired = set(extra_desired)
    for resource in resources:
        if resource.namespaced:
            if all_namespaces:
                desired.add(InformerRef(resource, ALL_NAMESPACES))
            else:
                for namespace in namespaces:
                    desired.add(InformerRef(resource, namespace))
        else:
            desired.add(InformerRef(resource, None))
    return InformerPlan(
        start=frozenset(desired - set(current)),
        stop=frozenset(set(current) - desired),
    )


def ref_for(resource: type) -> ResourceRef:
    """Derive a ResourceRef from a resource class."""
    from .resources import identity

    return ResourceRef(
        api_version=identity.api_version(resource),
        kind=identity.kind(resource),
        namespaced=identity.is_namespaced(resource),
    )
