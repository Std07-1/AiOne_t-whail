from __future__ import annotations

import asyncio
import inspect
from collections.abc import Generator
from unittest import mock

import pytest


class _SimpleMocker:
    """Мінімальний mocker із unittest.mock для тестів без pytest-mock."""

    def __init__(self) -> None:
        self._patchers: list[mock._patch] = []

    def patch(self, target: str, *args, **kwargs):
        patcher = mock.patch(target, *args, **kwargs)
        started = patcher.start()
        self._patchers.append(patcher)
        return started

    def patch_object(self, target: object, attribute: str, *args, **kwargs):
        patcher = mock.patch.object(target, attribute, *args, **kwargs)
        started = patcher.start()
        self._patchers.append(patcher)
        return started

    def stopall(self) -> None:
        while self._patchers:
            self._patchers.pop().stop()


@pytest.fixture
def mocker() -> Generator[_SimpleMocker, None, None]:
    helper = _SimpleMocker()
    try:
        yield helper
    finally:
        helper.stopall()


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    if inspect.iscoroutinefunction(pyfuncitem.obj):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(pyfuncitem.obj(**pyfuncitem.funcargs))
        finally:
            loop.close()
        return True
    return None
