import os
import pathlib

# Point KUBECONFIG at a fake file so tests never touch the real one.
# (The lightkube client is created lazily at operator start, so importing
# chopf no longer reads the kubeconfig — this is a safety net for tests
# that do start client-creating code paths.)
os.environ.setdefault(
    'KUBECONFIG',
    str(pathlib.Path(__file__).parent / 'fake-kubeconfig.yaml'),
)
