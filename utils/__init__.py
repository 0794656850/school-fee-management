from __future__ import annotations

from functools import wraps
from typing import Callable, TypeVar, Any, cast
from flask import session, redirect

F = TypeVar("F", bound=Callable[..., Any])


def admin_required(func: F) -> F:
    """Decorator that requires an admin session.

    - If ``session['admin_logged_in']`` is truthy, proceeds to the view.
    - Otherwise, redirects to ``/login``.

    This shape is kept minimal to satisfy existing tests and
    to be framework-agnostic across blueprints.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any):
        if not session.get("admin_logged_in"):
            return redirect("/login")
        return func(*args, **kwargs)

    return cast(F, wrapper)
