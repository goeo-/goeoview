from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

from .db import DB
from .logger import get_logger

logger = get_logger("goeoview.hooks")

MAX_CONSECUTIVE_FAILURES = 5


@dataclass
class RegisteredHook:
    callback: object  # async callable or None (for tests)
    source_file: str
    collections: set[str] | None = None
    ops: set[str] | None = None
    dids: set[str] | None = None
    consecutive_failures: int = 0
    disabled: bool = False

    def matches(self, commit: dict) -> bool:
        if self.disabled:
            return False
        if self.collections is not None and commit["collection"] not in self.collections:
            return False
        if self.ops is not None and commit["action"] not in self.ops:
            return False
        if self.dids is not None and commit["did"] not in self.dids:
            return False
        return True


class HookRegistry:
    def __init__(self):
        self.hooks: list[RegisteredHook] = []
        self._hooks_dir: Path | None = None

    def load(self, hooks_dir: str | Path) -> None:
        self._hooks_dir = Path(hooks_dir)
        self._do_load()

    def reload(self) -> None:
        if self._hooks_dir is None:
            return
        self._do_load()

    def _do_load(self) -> None:
        new_hooks: list[RegisteredHook] = []

        for py_file in sorted(self._hooks_dir.glob("*.py")):
            try:
                hooks_from_file = self._load_file(py_file)
                new_hooks.extend(hooks_from_file)
            except Exception:
                logger.exception("failed to load hook %s", py_file)

        self.hooks = new_hooks
        logger.info("loaded %d hooks from %s", len(self.hooks), self._hooks_dir)

    def _load_file(self, py_file: Path) -> list[RegisteredHook]:
        module_name = f"_hooks_.{py_file.stem}"

        # Remove old module so re-import works
        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, py_file)

        # Delete cached bytecode so changes to the source file are picked up
        if spec.cached:
            cached = Path(spec.cached)
            if cached.exists():
                cached.unlink()
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        register_fn = getattr(module, "register", None)
        if register_fn is None:
            logger.warning("hook %s has no register() function, skipping", py_file)
            return []

        collected: list[RegisteredHook] = []

        def hook_registrar(*, callback, collections=None, ops=None, dids=None):
            collected.append(RegisteredHook(
                callback=callback,
                source_file=str(py_file),
                collections=set(collections) if collections else None,
                ops=set(ops) if ops else None,
                dids=set(dids) if dids else None,
            ))

        register_fn(hook_registrar)
        return collected

    async def watch(self) -> None:
        """Watch the hooks directory for changes and reload on any change."""
        import watchfiles

        if self._hooks_dir is None:
            return

        logger.info("watching %s for hook changes", self._hooks_dir)
        async for _changes in watchfiles.awatch(self._hooks_dir):
            logger.info("hooks directory changed, reloading")
            self.reload()

    def dispatch(self, commits: list[dict]) -> None:
        if not commits or not self.hooks:
            return

        db = DB()
        for hook in self.hooks:
            matched = [c for c in commits if hook.matches(c)]
            if not matched:
                continue
            asyncio.create_task(self._run_hook(hook, matched, db))

    async def _run_hook(self, hook: RegisteredHook, commits: list[dict], db: DB) -> None:
        try:
            await hook.callback(commits, db)
            hook.consecutive_failures = 0
        except Exception:
            hook.consecutive_failures += 1
            logger.exception(
                "hook %s from %s failed (%d consecutive)",
                hook.callback.__name__, hook.source_file, hook.consecutive_failures,
            )
            if hook.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                hook.disabled = True
                logger.warning(
                    "hook %s from %s disabled after %d consecutive failures",
                    hook.callback.__name__, hook.source_file, hook.consecutive_failures,
                )
