import dataclasses
import functools
import logging
import typing

import anyio
from anyio import TASK_STATUS_IGNORED
from anyio.abc import CancelScope, TaskStatus

from lightkube.core import resource as lkr
from lightkube.models.meta_v1 import OwnerReference

from ..tasks import Task
from ..workqueue import Workqueue
from ..source import EventSource
from ..resources import get_resource
from ..invocation import is_async_fn
from ..exceptions import (
    Requeue,
    TemporaryError,
    PermanentError,
    ObjectNotFound,
)

from . import requests_from_event_for_object


log = logging.getLogger(__name__)


class ReconcilerLoggerAdapter(logging.LoggerAdapter):
    """Prefixes the log message with reconcilers number"""

    def process(self, msg, kwargs):
        reconciler = 'reconciler[%i]' % self.extra['num']
        return '%s: %s' % (reconciler, msg), kwargs


class Controller(Task):
    async_client: object
    sync_client: object
    cache: object
    resource: lkr.Resource
    name: str = None
    predicates: typing.List[typing.Callable] = dataclasses.field(default_factory=list)
    watches: dict = dataclasses.field(default_factory=dict)
    tasks: typing.List[typing.Callable] = dataclasses.field(default_factory=list)
    reconcile: typing.Callable = None
    startup: typing.Callable = None
    shutdown: typing.Callable = None
    concurrent_reconciles: int = None
    wait_for_cache: bool = True

    @property
    def api_version(self) -> str:
        return self.resource._api_info.resource.api_version

    @property
    def kind(self) -> str:
        return self.resource._api_info.resource.kind

    def __init__(self, async_client, sync_client, cache, resource,
        name=None,
        predicates=None, watches=None, tasks=None,
        reconcile=None, startup=None, shutdown=None,
        concurrent_reconciles=1, wait_for_cache=True):
        super().__init__()
        self.async_client = async_client
        self.sync_client = sync_client
        self.cache = cache
        self.resource = resource
        self.predicates = predicates or []
        self.watches = watches or {}
        self.tasks = tasks or []
        if reconcile:
            self.reconcile = reconcile
        if startup:
            self.startup = startup
        if shutdown:
            self.shutdown = shutdown
        self.concurrent_reconciles = concurrent_reconciles
        self.wait_for_cache = wait_for_cache

        self._task_group = None  # Main taskgroup
        self._stop = anyio.Event()
        self.resource = get_resource(self.resource)
        self._event_sources = []
        self.queue = Workqueue()
        self.lock = anyio.Lock()

        # Ensure we have a watch for the api_version/kind we are reconciling
        # if the user did not specify one explicitly.
        if self.resource not in self.watches:
            self._add_event_source(self.resource, requests_from_event_for_object)

        # Create event sources for all our watches.
        for watch in self.watches.values():
            self._add_event_source(
                watch['resource'],
                watch['handler'],
                **watch['kwargs'],
            )


    def __repr__(self):
        if self.name is not None:
            return f'<{self.__class__.__name__} {self.name} {self.resource.apiVersion}/{self.resource.kind}>'
        else:
            return f'<{self.__class__.__name__} {self.resource.apiVersion}/{self.resource.kind}>'

    @property
    def event_sources(self):
        return self._event_sources

    def set_owner_reference(
        self, owner, subject, block_owner_deletion=False, controller=False
    ):
        # ref = metav1.OwnerReference{
        #    APIVersion:         gvk.GroupVersion().String(),
        #    Kind:               gvk.Kind,
        #    Name:               owner.GetName(),
        #    UID:                owner.GetUID(),
        #    BlockOwnerDeletion: ptr.To(false),
        #    Controller:         ptr.To(false),
        # }
        if subject.metadata.ownerReferences is None:
            subject.metadata.ownerReferences = []
        if controller:
            for existing_ref in subject.metadata.ownerReferences:
                if existing_ref.controller:
                    raise Exception('Allready owned by a controller: %r', existing_ref)
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

    def set_controller_reference(self, owner, subject):
        return self.set_owner_reference(
            owner,
            subject,
            block_owner_deletion=True,
            controller=True,
        )

    def _add_event_source(self, resource, handler, **kwargs):
        #log.debug(f'_add_event_source {resource} {handler} {kwargs}')
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
            #logger.info(self.queue)
            request = await self.queue.get(num)
            logger.debug('processing %r', request)

            try:
                if is_async_fn(self.reconcile):
                    await self.reconcile(self.async_client, request)
                else:
                    await anyio.to_thread.run_sync(
                        self.reconcile, self.sync_client, request
                    )
            except ObjectNotFound as e:
                log.debug(e)
                # If the object is not in our cache, there's no point to
                # requeue the request. So we give up and forget about it.
                await self.queue.forget(request)
            except PermanentError as e:
                log.error(e)
                # The reconcile function signaled to us that it can not handle
                # this request so we give up and forget about it.
                await self.queue.forget(request)
            except TemporaryError as e:
                # Requeue this request after the requested delay.
                logger.debug('requeuing with delay %i %r', e.delay, request)
                request.retries += 1
                await self.queue.forget(request)
                await self.queue.add_after(request, e.delay)
            except Requeue as e:
                await self.queue.forget(request)
                if e.after:
                    logger.debug('requeuing with delay %i %r', e.after, request)
                    await self.queue.add_after(request, e.after)
                else:
                    logger.debug('requeuing %r', request)
                    await self.queue.add(request)
            except Exception as e:
                log.exception(e)
                #raise e
                # Unexpected error, log it and requeue with rate limiting.
                logger.debug('requeuing with rate limiting %r', request)
                request.retries = await self.queue.num_requeues(request)
                await self.queue.add_rate_limited(request)
            else:
                # Success! Forget about this request.
                await self.queue.forget(request)
            finally:
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
                        tg.start_soon(anyio.to_thread.run_sync,
                            functools.partial(
                                task,
                                self.sync_client,
                            )
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
                        for informer in self.cache.get_informers_by(resource=source.resource):
                            if not informer.has_stream(key=source):
                                informer.add_stream(source.stream, key=source)

                    #log.debug('started %s', self)
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
