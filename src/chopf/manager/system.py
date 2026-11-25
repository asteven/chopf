"""Internal runtime: the always-on part of the operator.

Runs the built-in controllers (CRDs, namespaces, and later leader
election) with a dedicated, static cache and their own clients —
independent of the manager's shared, membership-driven cache (the
"leader payload").

This separation serves two purposes:

- Startup ordering: the internal controllers observe the cluster (CRD
  discovery feeds resource registration, namespace observation feeds
  membership) and are fully synced before the payload — shared cache,
  resource verification, user controllers — is started.
- HA: a follower runs only this runtime — a couple of informers and
  controllers — and never pays memory/CPU for a shared cache nobody
  reads from. Leadership (LeaseController, once implemented) decides
  whether the payload runs alongside.
"""
import logging

from ..cache import Cache
from ..client import AsyncClient, SyncClient
from ..specs import ControllerSpec
from .controllers import CrdController, NamespaceController

log = logging.getLogger(__name__)


class InternalRuntime:
    """Owns the internal cache, clients and controllers."""

    def __init__(self, manager):
        self.manager = manager
        self.api_client = manager.async_api_client
        # A dedicated cache with static membership: informers for the
        # internal resources only, never driven by the membership planner.
        self.cache = Cache(self.api_client)
        self.cache.observer = manager.observer
        self.async_client = AsyncClient(
            self.api_client, self.cache, observer=manager.observer
        )
        self.sync_client = SyncClient(self.async_client)
        self._specs = []
        self._controllers = {}

    @property
    def controllers(self):
        return self._controllers

    def _materialize(self, spec):
        controller = spec.controller_class(
            self.manager,
            self.async_client,
            self.sync_client,
            self.cache,
            spec.resource,
            name=spec.name,
            predicates=list(spec.predicates),
            watches=list(spec.watches),
            tasks=list(spec.tasks),
            concurrent_reconciles=spec.concurrent_reconciles,
            wait_for_cache=spec.wait_for_cache,
            observer=self.manager.observer,
        )
        self._controllers[spec.resource] = controller
        return controller

    def _setup_controllers(self):
        """Register and materialize the built-in controllers."""
        specs = [ControllerSpec.from_class(CrdController)]
        if not self.manager.all_namespaces:
            specs.append(ControllerSpec.from_class(NamespaceController))
        # TODO: leader election is not implemented. Once it is, the
        #       LeaseController joins this runtime and gates the payload.
        for spec in specs:
            self._materialize(spec)
        self._specs = specs

    def _configure_cache(self):
        """Static membership: informers for the internal resources only."""
        self.cache.resources = {spec.resource for spec in self._specs}

    async def start(self, task_group):
        """Start the runtime inside the given task group.

        Returns once the internal controllers have been synced: informers
        have completed their initial list and the controllers have
        processed the initial cluster state.
        """
        self._configure_cache()

        # Start the cache task and create the informers, then start the
        # controllers so their event sources are connected before the
        # informers' initial list dispatches.
        await task_group.start(self.cache)
        self.cache.reconcile_informers()
        for controller in self._controllers.values():
            await task_group.start(controller)

        # Now let the informers list; cache.synced releases the
        # controllers' reconcilers.
        await self.cache.reconcile()

        # Wait until the controllers have processed the initial state
        # (CRDs registered, namespaces observed) before the payload starts.
        for controller in self._controllers.values():
            await controller.wait_idle()
        log.info('internal runtime started')

    def stop(self):
        for controller in self._controllers.values():
            controller.stop()
        self.cache.stop()
        log.info('internal runtime stopped')
