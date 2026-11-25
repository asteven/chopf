"""Declarative, immutable descriptions of runtime components.

Specs are collected at import time — via the builder decorators
(``chopf.controller()``, ``chopf.informer()``) or by introspecting
Controller subclasses — and are later materialized into running
components by the manager.

Specs are frozen on purpose: they describe *what* should run, and never
reference *running* instances. All runtime wiring happens in exactly one
place: the manager's materialization step.
"""

from __future__ import annotations

import dataclasses
import typing as t


@dataclasses.dataclass(frozen=True)
class WatchSpec:
    """A watch on a resource, feeding events through a handler."""

    resource: t.Any
    handler: t.Callable
    kwargs: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class ControllerSpec:
    """Everything needed to construct and wire a Controller.

    Produced either by the builder decorators (``controller_class=None``,
    handlers as plain callables) or by introspecting a Controller
    subclass (``controller_class`` set, handlers are its methods).
    """

    resource: t.Any
    name: str | None = None
    reconcile: t.Callable | None = None
    predicates: tuple = ()
    watches: tuple = ()
    tasks: tuple = ()
    startup: t.Callable | None = None
    shutdown: t.Callable | None = None
    concurrent_reconciles: int = 1
    wait_for_cache: bool = True
    controller_class: type | None = None

    @classmethod
    def from_class(cls, controller_class: type) -> ControllerSpec:
        """Build a spec from a Controller subclass' declared attributes."""
        resource = getattr(controller_class, 'resource', None)
        if resource is None:
            raise TypeError(
                f'{controller_class.__name__} must declare a resource class attribute'
            )
        return cls(
            resource=resource,
            controller_class=controller_class,
            name=getattr(controller_class, 'name', None),
            concurrent_reconciles=getattr(controller_class, 'concurrent_reconciles', 1),
            wait_for_cache=getattr(controller_class, 'wait_for_cache', True),
        )


@dataclasses.dataclass(frozen=True)
class InformerSpec:
    """Everything needed to construct and wire an Informer's receivers."""

    resource: t.Any
    name: str | None = None
    resync_after: float | None = None
    timeout: float = 10
    transformer: t.Callable | None = None
    stream_receivers: tuple = ()
