import functools
import logging
import time

import anyio
from anyio import TASK_STATUS_IGNORED, CancelScope
from anyio.abc import TaskStatus
from lightkube.core.exceptions import ApiError
from lightkube.models.meta_v1 import OwnerReference

from ..exceptions import (
    Error,
    ObjectNotFound,
    PermanentError,
    Requeue,
    TemporaryError,
)
from ..invocation import is_async_fn
from ..observer import Observer
from ..resources import identity
from ..source import EventSource
from ..tasks import Task
from ..workqueue import Workqueue
from . import requests_from_event_for_object

log = logging.getLogger(__name__)


class ReconcilerLoggerAdapter(logging.LoggerAdapter):
    """Prefixes the log message with reconcilers number"""

    def process(self, msg, kwargs):
        reconciler = f'reconciler[{self.extra["num"]}]'
        return f'{reconciler}: {msg}', kwargs


def set_owner_reference(owner, subject, block_owner_deletion=False, controller=False):
    """Append an OwnerReference for owner to the subjects metadata."""
    if subject.metadata.ownerReferences is None:
        subject.metadata.ownerReferences = []
    if controller:
        for existing_ref in subject.metadata.ownerReferences:
            if existing_ref.controller:
                raise Error(f'Allready owned by a controller: {existing_ref!r}')
    ref = OwnerReference(
        apiVersion=owner.apiVersion,
        kind=owner.kind,
        name=owner.metadata.name,
        uid=owner.metadata.uid,
        blockOwnerDeletion=block_owner_deletion,
        controller=controller,
    )
    subject.metadata.ownerReferences.append(ref)
    return ref


def set_controller_reference(owner, subject):
    """Set owner as the controlling OwnerReference of subject."""
    return set_owner_reference(
        owner,
        subject,
        block_owner_deletion=True,
        controller=True,
    )


class Controller(Task):
    # Defaults; subclasses may override these with methods, builders
    # inject them as constructor arguments.
    reconcile = None
    startup = None
    shutdown = None

    @property
    def api_version(self) -> str:
        return identity.api_version(self.resource)

    @property
    def kind(self) -> str:
        return identity.kind(self.resource)

    def __init__(
        self,
        async_client,
        sync_client,
        cache,
        resource,
        name=None,
        predicates=None,
        watches=None,
        tasks=None,
        reconcile=None,
        startup=None,
        shutdown=None,
        concurrent_reconciles=1,
        wait_for_cache=True,
        observer=None,
    ):
        super().__init__()
        self.async_client = async_client
        self.sync_client = sync_client
        self.cache = cache
        self.resource = resource
        self.name = name
        self.predicates = predicates or []
        self.watches = list(watches or [])
        self.tasks = tasks or []
        if reconcile is not None:
            self.reconcile = reconcile
        if startup is not None:
            self.startup = startup
        if shutdown is not None:
            self.shutdown = shutdown
        self.concurrent_reconciles = concurrent_reconciles
        self.wait_for_cache = wait_for_cache
        self.observer = observer or Observer()

        self._task_group = None  # Main taskgroup
        self._stop = anyio.Event()
        self._event_sources = []
        self.queue = Workqueue()
        self.lock = anyio.Lock()

        # Ensure we have a watch for the api_version/kind we are reconciling
        # if the user did not specify one explicitly.
        if not any(watch.resource == self.resource for watch in self.watches):
            self._add_event_source(self.resource, requests_from_event_for_object)

        # Create event sources for all our watches.
        for watch in self.watches:
            self._add_event_source(
                watch.resource,
                watch.handler,
                **watch.kwargs,
            )

    def __repr__(self):
        if self.name is not None:
            return (
                f'<{self.__class__.__name__} {self.name} {identity.gvk(self.resource)}>'
            )
        else:
            return f'<{self.__class__.__name__} {identity.gvk(self.resource)}>'

    @property
    def event_sources(self):
        return self._event_sources

    def set_owner_reference(
        self, owner, subject, block_owner_deletion=False, controller=False
    ):
        return set_owner_reference(
            owner,
            subject,
            block_owner_deletion=block_owner_deletion,
            controller=controller,
        )

    def set_controller_reference(self, owner, subject):
        return set_controller_reference(owner, subject)

    def _add_event_source(self, resource, handler, **kwargs):
        # log.debug(f'_add_event_source {resource} {handler} {kwargs}')
        source = EventSource(
            self.queue,
            resource,
            handler,
            kwargs,
            predicates=self.predicates,
        )
        self._event_sources.append(source)

    def get_index(self, index_name):
        store = self.cache.get_store(self.resource)
        return store.get_index(
            index_name,
            resource=self.resource,
        )

    async def _startup(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        log.debug('startup')
        if is_async_fn(self.startup):
            await self.startup(self.async_client)
        else:
            await anyio.to_thread.run_sync(self.startup, self.sync_client)
        task_status.started()

    async def _shutdown(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        log.debug('shutdown')
        if is_async_fn(self.shutdown):
            await self.shutdown(self.async_client)
        else:
            await anyio.to_thread.run_sync(self.shutdown, self.sync_client)
        task_status.started()

    async def _reconciler(self, num):
        log_vars = {'num': num}
        logger = ReconcilerLoggerAdapter(log, log_vars)
        logger.debug('started')
        while True:
            logger.debug(self.queue)
            # logger.info(self.queue)
            request = await self.queue.get(num)
            logger.debug('processing %r', request)

            start = time.monotonic()
            outcome = 'success'
            try:
                if is_async_fn(self.reconcile):
                    await self.reconcile(self.async_client, request)
                else:
                    await anyio.to_thread.run_sync(
                        self.reconcile, self.sync_client, request
                    )
            except ObjectNotFound as e:
                outcome = 'not_found'
                log.debug(e)
                # If the object is not in our cache, there's no point to
                # requeue the request. So we give up and forget about it.
                await self.queue.forget(request)
            except PermanentError as e:
                outcome = 'permanent'
                log.error(e)
                # The reconcile function signaled to us that it can not handle
                # this request so we give up and forget about it.
                await self.queue.forget(request)
            except TemporaryError as e:
                outcome = 'temporary'
                # Requeue this request after the requested delay.
                logger.debug('requeuing with delay %i %r', e.delay, request)
                request.retries += 1
                await self.queue.forget(request)
                await self.queue.add_after(request, e.delay)
            except Requeue as e:
                outcome = 'requeue'
                await self.queue.forget(request)
                if e.after:
                    logger.debug('requeuing with delay %i %r', e.after, request)
                    await self.queue.add_after(request, e.after)
                else:
                    logger.debug('requeuing %r', request)
                    await self.queue.add(request)
            except Exception as e:
                if isinstance(e, ApiError) and getattr(e.status, 'code', None) == 409:
                    outcome = 'conflict'
                    # Optimistic concurrency conflict: our cached view of
                    # the object was stale (e.g. another controller updated
                    # it in the meantime) or, on create, the object already
                    # existed. Reconciles are idempotent by contract, so
                    # this is an expected transient, not an error: requeue
                    # with rate limiting and let the next attempt succeed
                    # once the cache has caught up with the API server.
                    logger.debug('conflict, requeuing %r', request)
                else:
                    outcome = 'error'
                    log.exception('error while processing request %r', request)
                    # Unexpected error, log it and requeue with rate limiting.
                    logger.debug('requeuing with rate limiting %r', request)
                request.retries = await self.queue.num_requeues(request)
                await self.queue.add_rate_limited(request)
            else:
                # Success! Forget about this request.
                await self.queue.forget(request)
            finally:
                duration = time.monotonic() - start
                self.observer.on_reconcile(
                    identity.gvk(self.resource), request, duration, outcome
                )
                # In any case, mark this request as done.
                logger.debug('done processing %r', request)
                await self.queue.done(request)

    async def _run_reconcilers(self):
        if callable(self.reconcile):
            async with anyio.create_task_group() as tg:
                for num in range(self.concurrent_reconciles):
                    tg.start_soon(self._reconciler, num)

    async def _show_queue(self):
        while True:
            print(
                f'{self.queue}: dirty: {self.queue._dirty}, processing: {self.queue._processing}, queue: {self.queue._queue._items}'
            )
            await anyio.sleep(5)

    async def wait_idle(self, settle=0.05):
        """Wait until the work queue has drained: nothing queued, nothing
        being processed.

        Used to sequence startup phases: the manager starts the system
        controllers first and waits for them to process the initial
        cluster state before starting user controllers. The settle window
        covers events that are still in flight from informer streams to
        the queue — the queue must be observed idle twice in a row.
        """
        while True:
            while not self.queue.idle:
                await anyio.sleep(settle)
            await anyio.sleep(settle)
            if self.queue.idle:
                return

    def stop(self):
        if self._task_group:
            log.debug('stop %r', self)
            self._task_group.cancel_scope.cancel()
        self.reset_task()

    async def _run_tasks(self):
        if self.tasks:
            async with anyio.create_task_group() as tg:
                for task in self.tasks:
                    if is_async_fn(task):
                        tg.start_soon(task, self.async_client)
                    else:
                        tg.start_soon(
                            anyio.to_thread.run_sync,
                            functools.partial(
                                task,
                                self.sync_client,
                            ),
                        )

    async def __call__(self, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        log.debug('starting %s', self)

        try:
            async with anyio.create_task_group() as tg:
                self._task_group = tg

                try:
                    ##tg.start_soon(self._show_queue)

                    for source in self._event_sources:
                        await tg.start(source)
                        for informer in self.cache.get_informers_by(
                            resource=source.resource
                        ):
                            if not informer.has_stream(key=source):
                                informer.add_stream(source.stream, key=source)

                    # log.debug('started %s', self)
                    log.info('started %s', self)
                    # Inform any awaiters that we are ready.
                    task_status.started()
                    self._running.set()

                    await tg.start(self.queue)

                    if self.wait_for_cache:
                        await self.cache.synced

                    if self.startup is not None:
                        await tg.start(self._startup)

                    tg.start_soon(self._run_tasks)
                    tg.start_soon(self._run_reconcilers)

                    # Wait until told otherwise.
                    await self._stop.wait()

                except anyio.get_cancelled_exc_class():
                    log.debug('canceled %s', self)
                    raise

                finally:
                    log.debug('stopping %s', self)
                    if self.shutdown is not None:
                        with CancelScope(shield=True):
                            await tg.start(self._shutdown)

        finally:
            log.info('stopped %s', self)
