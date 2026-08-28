"""覆盖率 — script.py reload_loop 全分支"""
import asyncio
import os
import tempfile
import time
import pytest
from unittest.mock import patch

from evebus import EventEngine
from evebus.executors import ScriptExecutor


def _make_script(name, code):
    path = os.path.join(tempfile.gettempdir(), name)
    with open(path, "w") as f:
        f.write(code)
    return path


class TestReloadLoopBranches:

    @pytest.mark.asyncio
    async def test_reload_on_mtime_change(self):
        """mtime 变化触发重载（159-161 分支）"""
        path = _make_script("rl_mtime.py", 'v = 1\nasync def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path, auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        await asyncio.sleep(0.12)
        # 修改内容 + 强制 mtime 变化
        with open(path, "w") as f:
            f.write('v = 2\nasync def on_event(t, e): pass\n')
        os.utime(path, (time.time() + 5, time.time() + 5))
        await asyncio.sleep(0.3)
        assert ex._module.v == 2
        await engine.remove_executor("x")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_file_not_found(self):
        """文件被删 → FileNotFoundError 分支（164-166）"""
        path = _make_script("rl_nf.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path, auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        os.remove(path)
        await asyncio.sleep(0.15)  # reload loop 遇到 FileNotFoundError
        # 不崩溃，旧 handler 保留
        assert ex._on_event is not None
        await engine.remove_executor("x")

    @pytest.mark.asyncio
    async def test_reload_script_error_keeps_old(self):
        """新脚本有错误 → Exception 分支，保留旧 handler（167-169）"""
        path = _make_script("rl_err.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path, auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        await asyncio.sleep(0.12)
        with open(path, "w") as f:
            f.write('def broken(:\n')  # 语法错误
        os.utime(path, (time.time() + 5, time.time() + 5))
        await asyncio.sleep(0.3)
        # 旧 handler 仍在，加载失败被记录
        assert ex._on_event is not None
        await engine.remove_executor("x")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_loop_finally_resets_running(self):
        """循环结束 finally 复位 _running（170-171）"""
        path = _make_script("rl_fin.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path, auto_reload=True, reload_interval_sec=60)
        engine = EventEngine()
        await engine.add_executor(ex)
        await asyncio.sleep(0.05)  # 等 reload_loop task 启动
        assert ex._running is True
        await engine.remove_executor("x")  # stop 取消 task
        await asyncio.sleep(0.05)
        assert ex._running is False
        os.remove(path)

    @pytest.mark.asyncio
    async def test_manual_reload_after_write(self):
        """手动 reload 加载新内容（147）"""
        path = _make_script("rl_manual.py", 'v = 1\nasync def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path)
        ex._load_script()
        assert ex._module.v == 1
        with open(path, "w") as f:
            f.write('v = 2\nasync def on_event(t, e): pass\n')
        await ex.reload()
        assert ex._module.v == 2
        os.remove(path)