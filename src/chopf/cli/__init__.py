import logging
import pathlib
import sys

from typing import List
from typing_extensions import Annotated

import typer.core

typer.core.rich = None

import typer  # noqa: E402


from . import loaders  # noqa: E402


app = typer.Typer(add_completion=False)


#    config_file: Annotated[str, typer.Option('--config', envvar='CNS_CONFIG_FILE')] = None
@app.callback()
def main(
    ctx: typer.Context,
    verbose: Annotated[bool, typer.Option('--verbose', '-v')] = False,
    debug: Annotated[bool, typer.Option('--debug', '-d')] = False,
) -> None:
    """
    Chopf.
    """
    setattr(ctx, 'obj', {})

    logging.basicConfig(
        level=logging.ERROR,
        format='%(levelname)s: %(module)s: %(message)s',
        stream=sys.stderr,
    )
    log = logging.getLogger('chopf')
    log_level = logging.ERROR
    if verbose:
        log_level = logging.INFO
    elif debug:
        log_level = logging.DEBUG
    log.setLevel(log_level)
    ctx.obj['log_level'] = log_level
    ctx.obj['log'] = log


#    paths: Annotated[Optional[List[str]], typer.Argument(default_factory=list)],
#    paths: Annotated[List[pathlib.Path], typer.Argument(default_factory=list)],
@app.command(name='run', short_help='run')
def run(
    ctx: typer.Context,
    modules: Annotated[List[str], typer.Option()] = None,
    paths: Annotated[List[pathlib.Path], typer.Argument()] = None,
    all_namespaces: Annotated[
        bool, typer.Option('--all-namespaces', help='Watch all namespaces.')
    ] = False,
    namespaces: Annotated[
        List[str],
        typer.Option(
            '--namespace',
            help='Watch the given namespaces instead of the default. Can be given multiple times.',
        ),
    ] = None,
) -> None:
    paths = paths or []
    modules = modules or []
    loaders.preload(
        paths=paths,
        modules=modules,
    )
    from chopf import manager

    manager.run(all_namespaces=all_namespaces, namespaces=namespaces)


#    paths: Annotated[Optional[List[str]], typer.Argument(default_factory=list)],
#    paths: Annotated[List[pathlib.Path], typer.Argument(default_factory=list)],
#    paths: Annotated[List[pathlib.Path], typer.Argument()] = None,
@app.command(
    name='crd', short_help='Generate CRDs from the resources in the given modules'
)
def crd(
    ctx: typer.Context,
    modules: Annotated[List[str], typer.Option()] = None,
) -> None:
    modules = modules or []
    loaders.preload(
        modules=modules,
    )

    from chopf import resources

    crds = resources.CustomResourceRegistry.all_crds()
    crds_yaml = resources.resources_to_yaml(*crds)
    print(crds_yaml)


if __name__ == '__main__':
    app()
