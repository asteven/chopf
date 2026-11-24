import pytest

from lightkube.resources.core_v1 import Pod
from chopf.resources import get_resource


@pytest.fixture(autouse=True)
def ensure_pod_resource_defaults():
    resource = get_resource(Pod)
    resource.apiVersion = 'v1'
    resource.kind = 'Pod'
