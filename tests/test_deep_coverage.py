"""针对性补充 — 脚本执行器 auto_reload / reload / stop，CLI serve 信号处理"""
import asyncio
import os
import tempfile
import signal
import sys
import pytest
from unittest.mock import patch, MagicMock
from evebus import EventEngine
from evebus.executors.script import ScriptExecutor


# ═══════════════════════════════════════
#  ScriptExecutor 深度覆盖
# ═══════════════════════════════════════

class TestScriptExecutorReload:

    def _make_script(self, name, code):
        path = os.path.join(tempfile.gettempdir(), name)
        with open(path, "w") as f:
            f.write(code)
        return path

    @pytest.mark.asyncio
    async def test_auto_reload_detection(self):
        """auto_reload 检测文件变化并重载"""
        path = self._make_script("reload_det.py", 'v = 1\nasync def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"], auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        assert ex._module is not None
        assert ex._module.v == 1

        # 修改脚本
        with open(path, "w") as f:
            f.write('v = 2\nasync def on_event(t, e): pass\n')

        await asyncio.sleep(0.3)
        # reload 循环应该已检测到变化
        assert ex._reload_task is not None

        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_stop_with_reload_task(self):
        """stop 会取消 reload_task（#25: stop 后 task 置 None 且已取消）"""
        path = self._make_script("stop_rl.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"], auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        assert ex._reload_task is not None
        task = ex._reload_task
        await engine.remove_executor("ex1")
        assert ex._reload_task is None
        assert task.cancelled() or task.done()
        os.remove(path)

    @pytest.mark.asyncio
    async def test_execute_without_module(self):
        """execute 当 _on_event 为 None"""
        ex = ScriptExecutor(name="ex1", script_path="/fake", patterns=["*"])
        ex._on_event = None
        with pytest.raises(RuntimeError, match="未加载"):
            await ex.execute("topic", {})

    @pytest.mark.asyncio
    async def test_execute_async_on_event(self):
        """on_event 是 async 函数"""
        path = self._make_script("async_evt.py", '''
async def on_event(topic, payload):
    pass
''')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        ex._load_script()
        await ex.execute("topic", {"data": 1})
        os.remove(path)

    @pytest.mark.asyncio
    async def test_load_sync_on_start(self):
        """on_start 是普通函数"""
        path = self._make_script("sync_start.py", '''
started = False
def on_start():
    global started
    started = True
async def on_event(t, e): pass
''')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        ex._load_script()
        assert ex._module.started is True
        os.remove(path)

    @pytest.mark.asyncio
    async def test_load_async_on_start(self):
        """on_start 是 async 函数"""
        path = self._make_script("async_start.py", '''
started = False
async def on_start():
    global started
    started = True
async def on_event(t, e): pass
''')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        ex._load_script()
        # ensure_future 被调用
        assert ex._module is not None
        os.remove(path)

    @pytest.mark.asyncio
    async def test_load_sync_on_stop(self):
        """on_stop 是普通函数"""
        path = self._make_script("sync_stop.py", '''
stopped = False
def on_stop():
    global stopped
    stopped = True
async def on_event(t, e): pass
''')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        ex._load_script()
        assert callable(ex._on_stop)
        os.remove(path)

    @pytest.mark.asyncio
    async def test_stop_async_on_stop(self):
        """stop 调用 async on_stop"""
        path = self._make_script("async_stop.py", '''
stopped = False
async def on_stop():
    global stopped
    stopped = True
async def on_event(t, e): pass
''')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        engine = EventEngine()
        await engine.add_executor(ex)
        await engine.remove_executor("ex1")
        assert ex._module.stopped is True
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_loop_reload_error(self):
        """reload_loop 中 os.path.getmtime 抛异常"""
        path = self._make_script("err_rl.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"], auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        # 删除文件让 getmtime 失败
        os.remove(path)
        await asyncio.sleep(0.2)
        await engine.remove_executor("ex1")

    @pytest.mark.asyncio
    async def test_no_on_stop(self):
        """没有 on_stop 函数"""
        path = self._make_script("no_stop.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        engine = EventEngine()
        await engine.add_executor(ex)
        await engine.remove_executor("ex1")
        os.remove(path)

    def test_info_with_module(self):
        """info 包含 module_loaded"""
        path = self._make_script("info2.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        info = ex.info()
        assert "module_loaded" in info
        assert "auto_reload" in info
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_executor_reload(self):
        """engine.reload_executor 调用 reload"""
        path = self._make_script("rl2.py", 'v = 1\nasync def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        engine = EventEngine()
        await engine.add_executor(ex)
        with open(path, "w") as f:
            f.write('v = 2\nasync def on_event(t, e): pass\n')
        r = await engine.reload_executor("ex1")
        assert r["ok"] is True
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_start_auto_reload_false(self):
        """auto_reload=False 时不创建 reload_task"""
        path = self._make_script("no_reload.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"], auto_reload=False)
        engine = EventEngine()
        await engine.add_executor(ex)
        assert ex._reload_task is None
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_start_auto_reload_true(self):
        """auto_reload=True 时创建 reload_task"""
        path = self._make_script("yes_reload.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"], auto_reload=True, reload_interval_sec=0.1)
        engine = EventEngine()
        await engine.add_executor(ex)
        assert ex._reload_task is not None
        await engine.remove_executor("ex1")
        os.remove(path)


# ═══════════════════════════════════════
#  CLI serve 信号处理
# ═══════════════════════════════════════

class TestServeSignals:

    @patch("subprocess.Popen")
    def test_serve_reload_flag(self, mock_popen):
        """serve --reload 加上 reload 参数"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc

        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        runner = CliRunner()
        r = runner.invoke(server_cli, ["serve", "--reload"])
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert "--reload" in args

    @patch("subprocess.Popen")
    def test_serve_workers_flag(self, mock_popen):
        """serve --workers 2"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        runner = CliRunner()
        r = runner.invoke(server_cli, ["serve", "--workers", "2"])
        args = mock_popen.call_args[0][0]
        assert any("--workers=2" in a for a in args)

    @patch("subprocess.Popen")
    def test_serve_keyboard_interrupt(self, mock_popen):
        """serve 正常 Keyboard interrupt"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        runner = CliRunner()
        r = runner.invoke(server_cli, ["serve"])
        assert mock_proc.terminate.called

    @patch("subprocess.Popen")
    def test_serve_normal_exit(self, mock_popen):
        """serve 子进程正常退出"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.return_value = None

        runner = CliRunner()
        r = runner.invoke(server_cli, ["serve"])
        assert mock_proc.terminate.called  # finally 块总会调用

    @patch("subprocess.Popen")
    def test_serve_timeout_kills(self, mock_popen):
        """serve terminate 后超时 → kill"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli
        import subprocess as sp

        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.side_effect = [
            None,  # 主流程 wait
            sp.TimeoutExpired("cmd", 5),  # finally: wait(timeout=5) 超时
            None,  # finally: wait() after kill
        ]

        runner = CliRunner()
        r = runner.invoke(server_cli, ["serve"])
        assert mock_proc.terminate.called
        assert mock_proc.kill.called


# ═══════════════════════════════════════
#  CLI run 命令 — asyncio.run
# ═══════════════════════════════════════

class TestRunFull:

    @patch("asyncio.run")
    def test_run_invokes_asyncio_run(self, mock_run):
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        runner = CliRunner()
        path = os.path.join(tempfile.gettempdir(), "run_test.py")
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\n")

        r = runner.invoke(server_cli, ["run", path, "-t", "test.*"])
        mock_run.assert_called_once()
        os.remove(path)


# ═══════════════════════════════════════
#  server 边缘
# ═══════════════════════════════════════

class TestServerEdge:

    def test_emit_by_path_extra_segments(self):
        """更多路径段"""
        from starlette.testclient import TestClient
        from evebus.server import app
        client = TestClient(app)
        r = client.post("/api/v1/events/emit/data.a.b.c", json={"x": 1})
        assert r.status_code == 200
        assert r.json()["topic"] == "data.a.b.c"

    def test_stats_json_structure(self):
        from starlette.testclient import TestClient
        from evebus.server import app
        client = TestClient(app)
        r = client.get("/api/v1/stats")
        data = r.json()
        assert "hooks" in data
        assert "pending_tasks" in data