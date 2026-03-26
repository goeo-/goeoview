import asyncio

import pytest
from goeoview.hooks import RegisteredHook


def _make_commit(**overrides):
    """Build a commit dict with sensible defaults."""
    commit = {
        "did": "did:plc:test123",
        "collection": "app.bsky.feed.post",
        "rkey": "abc123",
        "rev": 100,
        "value": b'{"text":"hello"}',
        "since": 50,
        "action": "create",
    }
    commit.update(overrides)
    return commit


class TestRegisteredHookMatches:
    def test_no_filters_matches_everything(self):
        hook = RegisteredHook(callback=None, source_file="test.py")
        assert hook.matches(_make_commit())

    def test_collection_filter_matches(self):
        hook = RegisteredHook(
            callback=None, source_file="test.py",
            collections={"app.bsky.feed.post"},
        )
        assert hook.matches(_make_commit(collection="app.bsky.feed.post"))
        assert not hook.matches(_make_commit(collection="app.bsky.feed.like"))

    def test_ops_filter_matches(self):
        hook = RegisteredHook(
            callback=None, source_file="test.py",
            ops={"create"},
        )
        assert hook.matches(_make_commit(action="create"))
        assert not hook.matches(_make_commit(action="delete"))

    def test_dids_filter_matches(self):
        hook = RegisteredHook(
            callback=None, source_file="test.py",
            dids={"did:plc:test123"},
        )
        assert hook.matches(_make_commit(did="did:plc:test123"))
        assert not hook.matches(_make_commit(did="did:plc:other"))

    def test_multiple_filters_are_and(self):
        hook = RegisteredHook(
            callback=None, source_file="test.py",
            collections={"app.bsky.feed.post"},
            ops={"create"},
        )
        assert hook.matches(_make_commit(collection="app.bsky.feed.post", action="create"))
        assert not hook.matches(_make_commit(collection="app.bsky.feed.post", action="delete"))
        assert not hook.matches(_make_commit(collection="app.bsky.feed.like", action="create"))

    def test_disabled_hook_does_not_match(self):
        hook = RegisteredHook(callback=None, source_file="test.py")
        hook.disabled = True
        assert not hook.matches(_make_commit())


from goeoview.hooks import HookRegistry


class TestHookRegistryLoad:
    def test_load_empty_directory(self, tmp_path):
        registry = HookRegistry()
        registry.load(tmp_path)
        assert registry.hooks == []

    def test_load_hook_module(self, tmp_path):
        hook_file = tmp_path / "my_hook.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook):\n"
            "    hook(callback=cb, collections=['app.bsky.feed.post'])\n"
        )
        registry = HookRegistry()
        registry.load(tmp_path)
        assert len(registry.hooks) == 1
        assert registry.hooks[0].collections == {"app.bsky.feed.post"}
        assert registry.hooks[0].source_file == str(hook_file)

    def test_load_multiple_hooks_from_one_file(self, tmp_path):
        hook_file = tmp_path / "multi.py"
        hook_file.write_text(
            "async def cb1(commits, db): pass\n"
            "async def cb2(commits, db): pass\n"
            "def register(hook):\n"
            "    hook(callback=cb1, collections=['app.bsky.feed.post'])\n"
            "    hook(callback=cb2, ops=['delete'])\n"
        )
        registry = HookRegistry()
        registry.load(tmp_path)
        assert len(registry.hooks) == 2

    def test_load_skips_broken_file(self, tmp_path):
        good = tmp_path / "good.py"
        good.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook):\n"
            "    hook(callback=cb)\n"
        )
        bad = tmp_path / "bad.py"
        bad.write_text("this is not valid python ^^^")

        registry = HookRegistry()
        registry.load(tmp_path)
        assert len(registry.hooks) == 1

    def test_load_skips_file_without_register(self, tmp_path):
        hook_file = tmp_path / "no_register.py"
        hook_file.write_text("x = 1\n")
        registry = HookRegistry()
        registry.load(tmp_path)
        assert registry.hooks == []

    def test_reload_replaces_hooks(self, tmp_path):
        hook_file = tmp_path / "my_hook.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook):\n"
            "    hook(callback=cb, collections=['app.bsky.feed.post'])\n"
        )
        registry = HookRegistry()
        registry.load(tmp_path)
        assert len(registry.hooks) == 1
        assert registry.hooks[0].collections == {"app.bsky.feed.post"}

        # Modify the hook
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook):\n"
            "    hook(callback=cb, collections=['app.bsky.feed.like'])\n"
        )
        registry.reload()
        assert len(registry.hooks) == 1
        assert registry.hooks[0].collections == {"app.bsky.feed.like"}


class TestHookRegistryDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_calls_matching_hooks(self, tmp_path):
        received = []

        async def cb(commits, db):
            received.extend(commits)

        registry = HookRegistry()
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb)\n"
        )
        registry.load(tmp_path)
        registry.hooks[0].callback = cb

        commits = [_make_commit()]
        registry.dispatch(commits)
        await asyncio.sleep(0.01)
        assert received == commits

    @pytest.mark.asyncio
    async def test_dispatch_filters_commits_per_hook(self, tmp_path):
        received = []

        async def cb(commits, db):
            received.extend(commits)

        registry = HookRegistry()
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb, collections=['app.bsky.feed.post'])\n"
        )
        registry.load(tmp_path)
        registry.hooks[0].callback = cb

        commits = [
            _make_commit(collection="app.bsky.feed.post"),
            _make_commit(collection="app.bsky.feed.like"),
        ]
        registry.dispatch(commits)
        await asyncio.sleep(0.01)
        assert len(received) == 1
        assert received[0]["collection"] == "app.bsky.feed.post"

    @pytest.mark.asyncio
    async def test_dispatch_skips_disabled_hooks(self, tmp_path):
        received = []

        async def cb(commits, db):
            received.extend(commits)

        registry = HookRegistry()
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb)\n"
        )
        registry.load(tmp_path)
        registry.hooks[0].callback = cb
        registry.hooks[0].disabled = True

        registry.dispatch([_make_commit()])
        await asyncio.sleep(0.01)
        assert received == []

    @pytest.mark.asyncio
    async def test_dispatch_no_matching_commits_does_not_call(self, tmp_path):
        called = []

        async def cb(commits, db):
            called.append(True)

        registry = HookRegistry()
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb, collections=['app.bsky.feed.post'])\n"
        )
        registry.load(tmp_path)
        registry.hooks[0].callback = cb

        registry.dispatch([_make_commit(collection="app.bsky.feed.like")])
        await asyncio.sleep(0.01)
        assert called == []


class TestAutoDisable:
    @pytest.mark.asyncio
    async def test_hook_disabled_after_consecutive_failures(self, tmp_path):
        async def failing_cb(commits, db):
            raise RuntimeError("boom")

        registry = HookRegistry()
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb)\n"
        )
        registry.load(tmp_path)
        registry.hooks[0].callback = failing_cb

        for _ in range(5):
            registry.dispatch([_make_commit()])
            await asyncio.sleep(0.01)

        assert registry.hooks[0].disabled is True
        assert registry.hooks[0].consecutive_failures == 5

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self, tmp_path):
        call_count = 0

        async def sometimes_fails(commits, db):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise RuntimeError("boom")

        registry = HookRegistry()
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb)\n"
        )
        registry.load(tmp_path)
        registry.hooks[0].callback = sometimes_fails

        for _ in range(3):
            registry.dispatch([_make_commit()])
            await asyncio.sleep(0.01)
        assert registry.hooks[0].consecutive_failures == 3
        assert registry.hooks[0].disabled is False

        registry.dispatch([_make_commit()])
        await asyncio.sleep(0.01)
        assert registry.hooks[0].consecutive_failures == 0

    def test_reload_resets_disabled_state(self, tmp_path):
        hook_file = tmp_path / "test.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook): hook(callback=cb)\n"
        )
        registry = HookRegistry()
        registry.load(tmp_path)
        registry.hooks[0].disabled = True
        registry.hooks[0].consecutive_failures = 5

        registry.reload()
        assert registry.hooks[0].disabled is False
        assert registry.hooks[0].consecutive_failures == 0


class TestEndToEnd:
    @pytest.mark.asyncio
    async def test_full_load_and_dispatch_cycle(self, tmp_path):
        """Load a hook from a file, dispatch commits, verify the callback ran."""
        results_file = tmp_path / "results.txt"
        hook_file = tmp_path / "write_hook.py"
        hook_file.write_text(
            f"RESULTS_FILE = {str(results_file)!r}\n"
            "async def on_post(commits, db):\n"
            "    with open(RESULTS_FILE, 'a') as f:\n"
            "        for c in commits:\n"
            "            f.write(f\"{c['did']} {c['collection']} {c['action']}\\n\")\n"
            "\n"
            "def register(hook):\n"
            "    hook(\n"
            "        callback=on_post,\n"
            "        collections=['app.bsky.feed.post'],\n"
            "        ops=['create'],\n"
            "    )\n"
        )

        registry = HookRegistry()
        registry.load(tmp_path)

        commits = [
            _make_commit(did="did:plc:alice", collection="app.bsky.feed.post", action="create"),
            _make_commit(did="did:plc:bob", collection="app.bsky.feed.like", action="create"),
            _make_commit(did="did:plc:carol", collection="app.bsky.feed.post", action="delete"),
        ]
        registry.dispatch(commits)

        await asyncio.sleep(0.05)

        lines = results_file.read_text().strip().split("\n")
        assert len(lines) == 1
        assert "did:plc:alice app.bsky.feed.post create" in lines[0]


import time


class TestFileWatcher:
    @pytest.mark.asyncio
    async def test_watcher_reloads_on_file_change(self, tmp_path):
        hook_file = tmp_path / "my_hook.py"
        hook_file.write_text(
            "async def cb(commits, db): pass\n"
            "def register(hook):\n"
            "    hook(callback=cb, collections=['app.bsky.feed.post'])\n"
        )

        registry = HookRegistry()
        registry.load(tmp_path)
        assert len(registry.hooks) == 1
        assert registry.hooks[0].collections == {"app.bsky.feed.post"}

        watcher_task = asyncio.create_task(registry.watch())

        try:
            # Let the watcher task start up and register with inotify
            await asyncio.sleep(0.1)

            # Modify the hook file
            time.sleep(0.05)  # ensure mtime changes
            hook_file.write_text(
                "async def cb(commits, db): pass\n"
                "def register(hook):\n"
                "    hook(callback=cb, collections=['app.bsky.feed.like'])\n"
            )

            # Give the watcher time to detect and reload
            await asyncio.sleep(1.0)

            assert len(registry.hooks) == 1
            assert registry.hooks[0].collections == {"app.bsky.feed.like"}
        finally:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass
