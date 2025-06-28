"""
CLI entry point, when used as a module: `python -m chopf`.

Useful for debugging in the IDEs (use the start-mode "Module", module "chopf").
"""

import sys
from .cli import app

if __name__ == '__main__':
    sys.exit(app())
