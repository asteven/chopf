import datetime
from dataclasses import field
from typing import Annotated

from lightkube.models.core_v1 import PodTemplateSpec

from chopf import crd
from chopf.resources import ObjectMeta


@crd.model
class ChaosPodSpec:
    """ChaosPodSpec defines the desired state of ChaosPod."""

    template: PodTemplateSpec
    lifetime: Annotated[int, 'pod lifetime in seconds'] = 20


@crd.subresource
class ChaosPodStatus:
    """ChaosPodStatus defines the observed state of ChaosPod.
    It should always be reconstructable from the state of the cluster
    and/or outside world.
    """

    lastRun: datetime.datetime = None
    nextStop: datetime.datetime = None


@crd.resource(
    group='examples.chopf',
    version='v1alpha1',
    scope='Namespaced',
    served=True,
    storage=True,
)
@crd.printcolumn('next stop', '.status.nextStop', format='date')
@crd.printcolumn('last run', '.status.lastRun', format='date')
class ChaosPod:
    """ChaosPod is the Schema for the randomjobs API."""

    metadata: ObjectMeta
    spec: ChaosPodSpec
    status: ChaosPodStatus = field(default_factory=ChaosPodStatus)
