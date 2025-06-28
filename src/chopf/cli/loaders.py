"""
Module- and file-loading to trigger the handlers to be registered.

Since the framework is based on the decorators to register the handlers,
the files/modules with these handlers should be loaded first,
thus executing the decorators.

The files/modules to be loaded are usually specified on the command-line.
Currently, two loading modes are supported, both are equivalent to Python CLI:

* Plain files files (`chopf run file.py`).
* Importable modules (`chopf run -m pkg.mod`).

Multiple files/modules can be specified. They will be loaded in the order.
"""

import importlib
import importlib.abc
import importlib.util
import pkgutil
import os.path
import sys
from typing import Iterable, cast


def import_module(package_name):
    importlib.import_module(package_name)
    package = sys.modules[package_name]
    if hasattr(package, '__path__'):
        for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
            module_name = package_name + '.' + name
            if is_pkg:
                import_module(module_name)
            else:
                importlib.import_module(module_name)


def preload(
    paths: Iterable[str] = None,
    modules: Iterable[str] = None,
) -> None:
    """
    Ensure the handlers are registered by loading/importing the files/modules.
    """

    if paths is not None:
        for idx, path in enumerate(paths):
            if str(path).endswith('__pycache__'):
                continue
            sys.path.insert(0, os.path.abspath(os.path.dirname(path)))
            name = f'__chopf_script_{idx}__{path}'  # same pseudo-name as '__main__'
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec) if spec is not None else None
            loader = (
                cast(importlib.abc.Loader, spec.loader) if spec is not None else None
            )
            if module is not None and loader is not None:
                sys.modules[name] = module
                loader.exec_module(module)
            else:
                raise ImportError(f'Failed loading {path}: no module or loader.')

    if modules is not None:
        for name in modules:
            import_module(name)
