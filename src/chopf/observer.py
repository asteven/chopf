"""Observer hooks: the single instrumentation seam.

One observer sees everything worth measuring in the runtime:

- on_api_call: every API server call through the client facade
  (verb, resource, duration, error)
- on_watch_event: every event an informer processes (resource, type)
- on_reconcile: every reconcile invocation (resource, request, duration,
  outcome)

Assign your own observer before run():

    class MetricsObserver(chopf.Observer):
        def on_reconcile(self, gvk, request, duration, outcome):
            reconcile_histogram.labels(gvk=gvk, outcome=outcome).observe(duration)

    manager.observer = MetricsObserver()

The default Observer is a no-op; override only the hooks you need.
Reconcile outcomes: 'success', 'requeue', 'requeue_after', 'temporary',
'permanent', 'not_found', 'error'.
"""


class Observer:
    """No-op base class for instrumentation hooks."""

    def on_api_call(self, verb: str, gvk: str, duration: float, error=None) -> None:
        pass

    def on_watch_event(self, gvk: str, event_type: str) -> None:
        pass

    def on_reconcile(self, gvk: str, request, duration: float, outcome: str) -> None:
        pass


__all__ = [
    'Observer',
]
