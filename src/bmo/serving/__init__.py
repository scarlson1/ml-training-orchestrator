from __future__ import annotations

from typing import Any


__all__ = ['app']


def __getattr__(name: str) -> Any:
    """Lazily expose the FastAPI app without importing serving dependencies."""
    if name == 'app':
        from bmo.serving.api import app

        return app
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
