import pytest
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Pod


@pytest.fixture
def pod_factory():
    def _factory(name: str, namespace: str | None = 'default'):
        metadata = ObjectMeta(name=name, namespace=namespace)
        pod = Pod(metadata=metadata)
        pod.apiVersion = 'v1'
        pod.kind = 'Pod'
        return pod

    return _factory
